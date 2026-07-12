# V1.5 Historical Mature-Root Discovery

## Scope

This is an offline inventory of existing V1.5 queue evidence. It discovers and ranks exact queue roots, but it does not attest them, authorize fitting, or reinterpret segmented points as one continuous run.

The scan indexed:

- 85 `queue_summary.json` files under `D:\gas_calibrator`;
- 77 `queue_manifest.csv` files under `D:\gas_calibrator`;
- the other gas/calibration-related top-level directories on `D:\` for additional queue summaries;
- the existing `D:\_gas_calibrator_archive\20260622_first_gate` tree at file-name level.

No additional queue summary was found in the other gas/calibration directories. No file path in the current project or the reviewed archive contains an original `20260613`, `20260620`, or `20260621` run date. Paths containing `0613`, `0620`, or `0621` are later labels, primarily July fitting reviews, smoke points, and segmented route runs.

The reproducible queue index used by the exporter is produced with:

```powershell
rg --files D:\gas_calibrator -g 'queue_summary.json' | Sort-Object -Unique
```

## Result

```text
overall_status=blocked_no_complete_mature_root
summary_count=85
attestation_input_candidate_count=0
dry_run_only=7
forbidden_source=50
review_required=28
historical_fit_allowed=false
```

The important full-count findings are:

- `co2_6old_0620clean_mature45_g2` declares 45 points but has no closed manifest, final counts, or formal route-readiness evidence.
- `co2_6old_0620clean_mature45_g3_finalparams` is a partially executed/segmented root with non-ok points and incomplete samples/QC. It is not a continuous 45-point root.
- the 20260624 legacy CO2 root closes at 43/45 and is additionally bound to migration/direct provenance.
- H2O `g1/g2/g3` never produced a valid 13-point manifest; `g4` stopped after an early failure; `g5` is an 11-point remainder segment. These cannot be merged into a continuous 13-point attestation.
- no 47-point CO2 or 14-point H2O absorption candidate queue summary exists in the indexed evidence.

The accepted composite manifests created after segmented runs remain useful diagnostic/fitting-review evidence, but they are not equivalent to one uninterrupted mature route root and are not forwarded to the attestation binder.

## Safety Boundary

Discovery emits an empty `v1_5_historical_attestation_candidate_replay.json` for the current machine. It never opens COM, controls pressure or gas/water routes, writes identity or coefficients, connects PostgreSQL, permits archive release, permits import, or permits historical fitting.

The next offline step should not fabricate a mature root. It should classify legacy-format point data separately from queue evidence and preserve segmented accepted manifests as diagnostic-only lineage. A future continuous run, or a genuinely archived complete 0613/0620/0621 root, can be rescanned and then passed to the attestation binder.
