# V1.5 P1 Evidence Lineage Audit Review

## Decision

The two P1 core-evidence gaps from the legacy task plan have been resolved as lineage decisions, not by copying or manufacturing data.

## p017 T20 200 ppm

- Original status: failed
- Failure category: `dewpoint_rebound`
- Failure reason: `dewpoint_rebound_detected;max_total_wait_exceeded`
- Route opened: yes
- Formal sample window started: no
- Formal samples/frame QC/component QC: absent
- Raw IO: present, diagnostic-only
- Bounded same-lineage physical candidates: no usable retry/recovery
- Dry-run manifest reference: present, explicitly `dry_run_reference_only`
- Conclusion: `unrecoverable_from_reviewed_lineage`

This point must remain a failed raw diagnostic attempt. It cannot borrow samples or QC from another run, algorithm, or physical point.

## p006 Tm20 1000 ppm

- Original status: failed before sampling
- Failure category: `subprocess_failed`
- Recorded physical failure: `OPEN_FLOW_PRESSURE_HARD_LIMIT_EXCEEDED:PACE=1548.543hPa`
- Original formal sample window started: no
- Same-run sibling: `p006_Tm20_1000ppm_fit_retry1`
- Retry samples: present
- Retry frame QC: present
- Retry sidecar and completed route timing: present
- Retry component QC: absent
- Conclusion: `core_gap_resolved_by_same_lineage_retry_reference`

The retry closes only the original attempt's missing core-sample question. It remains explicit retry lineage and requires a separate component-QC review before any fit-input discussion.

## Search Boundary

The audit searched only each point's reviewed lineage root, direct run directories, direct point siblings, and bounded queue manifests. It did not search the whole disk or bind cross-run evidence.

## Locks

- `cross_run_search_performed=false`
- `cross_run_direct_bind_allowed=false`
- `automatic_file_copy_allowed=false`
- `automatic_qc_derivation_allowed=false`
- `continuous_route_attestation_allowed=false`
- `historical_fit_allowed=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`
- `not_real_acceptance_evidence=true`

## Next Action

Remove neither original directory. Treat p017 as permanently unavailable for fit input. Move only the p006 retry reference into the later P2 component-QC design review; do not copy its files into the failed original point directory.

## Validation

Focused compatibility coverage included the P1 lineage audit, legacy task plan and catalog, historical QC-gap audit, fit-input normalizer, route attestation binder, mature continuity gate, and entrypoint inventory.

Result: `96 passed, 1 existing marker warning in 37.93s`.

Static validation: ruff passed, Python compilation passed, and `git diff --check` passed apart from Windows line-ending notices.
