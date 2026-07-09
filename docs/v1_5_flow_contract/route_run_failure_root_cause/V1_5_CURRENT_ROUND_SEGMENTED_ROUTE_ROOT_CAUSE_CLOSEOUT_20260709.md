# V1.5 Current Round Segmented Route Root-Cause Closeout

This closeout records why the current CO2/H2O route did not run continuously and how V1.5 should prevent the same confusion in future formal production runs. It is offline evidence only: no COM, no route control, no PostgreSQL, no coefficient writes, and no release/import permission.

## Baseline

- Mature physical route baseline: `0613 fitting method + 0620/0621 final route execution`.
- Forbidden execution baselines: root migration worktree, `_handoff`, 0624 migration queue, diagnostic/probe tools, worker entrypoints, V1/V2 fallback surfaces.
- Old-algorithm production point counts remain `CO2 45` and `H2O 13`.
- A segmented recovery run can be reviewed for fitting only through an accepted/supersedence manifest. It must not be called a continuous formal run.

## Root-Cause Mapping

| Segment | Symptom | Root cause | Physical/program meaning | Required resolution |
|---|---|---|---|---|
| `g3_finalparams` 40C/400 | `dewpoint_rebound_detected;max_total_wait_exceeded` | Dry-gas route dewpoint was not stable enough before sampling. | CO2 zero gas residual water was unstable or the dry route re-wetted. | The failed point is not fit-eligible; only a reviewed retry/accepted manifest can replace it. |
| `g3_finalparams` 40C/1000 | Queue manifest left point as `running`, but point artifacts later existed. | Queue manifest finalization was stale after point artifacts were written. | The point may have data, but the queue cannot prove continuous execution. | Require accepted manifest/supersedence review; do not treat original queue as closed. |
| `g4_T30_to_Tm20_finalparams` 20C/300 | PACE vent `NO_RESPONSE`. | PACE atmosphere vent command failed during startup/pre-point reset. | The route could not prove atmospheric vent-hold before opening the point. | Stop queue, recover PACE communication/vent state, rerun from clean queue segment. |
| `g8/g8b/...` 20C 500-1000 | Queue attempts left `running`; points then moved to direct/retry runs with `notemp`/`240purge`. | Queue-level state did not close and execution parameters changed during recovery. | These points may be useful only as explicitly reviewed recovery evidence, not as one frozen queue. | Require parameter review plus accepted manifest binding each direct/retry point to the point it supersedes. |
| `g9/g10/g11` 10C to -20C | `-20C/400` failed then retry; `-20C/1000` direct recovery. | Pressure gauge/PACE readback had `NO_RESPONSE`, then recovery was performed outside the original queue closure. | The physical point sequence was interrupted; later points are recovery evidence. | Require recovery points in accepted manifest; block raw failed/running rows from fitting. |
| H2O `g1/g2/g3` | Planned 13, `ok=0`, `failed=0`, no `queue_manifest.csv`; `queue_abort_exclusion.csv` says aborted. | Queue aborted before effective point sampling and manifest creation. | `ok=0 failed=0` is not a pass; no formal point evidence was produced. | Treat as not executed; restart with canonical H2O queue and require queue manifest. |
| H2O `g4` | Only 2 points; 0C/50RH ok, 10C/30RH failed with PACE vent `NO_RESPONSE`. | Same PACE atmosphere vent failure as CO2. | H2O flow path could not prove open-flow pressure/vent state. | Stop and recover PACE; failed point is not fit-eligible. |
| H2O `g5` | Remaining 11 points completed. | Recovery segment after earlier aborted/failed segments. | Valid only as part of a reviewed segmented accepted manifest. | Require accepted H2O 13-point manifest and supersedence record before fitting. |

## Program-Level Fix Added

The new root-cause audit reads existing run folders and automatically classifies:

- `dry_gas_dewpoint_rebound_or_not_dry_enough`
- `pressure_controller_vent_no_response`
- `pressure_gauge_no_response`
- `stale_running_manifest_with_completed_point_artifacts`
- `running_manifest_without_completed_point_artifacts`
- `queue_aborted_before_sampling_no_manifest`
- `direct_or_retry_point_without_queue_manifest`
- `manual_parameter_or_execution_mode_change`

This is paired with the formal continuity gate. The continuity gate decides whether a run is continuous/pass, segmented/review-required, or blocked. The root-cause audit explains why each segment split or failed.

## Current-Round Audit Result

The live evidence audit over the named g3/g4/g8/g9/g10/g11 and H2O g1-g5 folders returned:

- status: `blocked`
- blocker_count: `12`
- review_required_count: `21`
- unclassified failure count: `0`

Important category counts:

- dry-gas dewpoint rebound: `1`
- PACE vent `NO_RESPONSE`: `3`
- pressure gauge `NO_RESPONSE`: `1`
- stale `running` manifest with completed point artifacts: `2`
- running manifest without completed point artifacts: `2`
- queue aborted before manifest: `3`
- direct/retry point without queue manifest: `8`
- manual parameter/execution mode change: `13`

## How This Prevents Recurrence

Before fitting or writeback, the formal run package must pass:

1. Production entrypoint gate: no `_handoff`, root migration, 0624, diagnostic, worker, V1/V2 references.
2. Formal run continuity gate: continuous queue pass or reviewed accepted/supersedence manifest.
3. Route run root-cause audit: no unresolved blockers such as dewpoint rebound, PACE/pressure `NO_RESPONSE`, stale running manifests, aborted no-manifest runs, or unbound direct/retry points.

If any of these fail, the process must not proceed as a continuous formal run. Recovery data can still be used only after explicit accepted-manifest review with point-level supersedence.
