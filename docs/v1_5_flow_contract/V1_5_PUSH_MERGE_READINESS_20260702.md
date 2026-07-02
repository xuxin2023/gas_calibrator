# V1.5 Push / Merge Readiness - 2026-07-02

This note is a reviewer-facing summary for the V1.5 clean worktree after the structure, contract, replay, algorithm-profile, and database-lock packages. It is not real acceptance evidence and does not authorize live queue execution, PostgreSQL import, or coefficient writes.

## Scope

- Worktree: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean`
- Branch: `codex/v1.5-original-flow-key-trace`
- Upstream: `origin/codex/v1.5-original-flow-key-trace`
- Last code-package HEAD before this readiness note: `52ebe14d Refresh V1.5 formal flow inventory contract`
- Ahead count including this readiness note: `75` commits
- Root workspace `D:\gas_calibrator` remains isolated and must not be used as the formal V1.5 source.

## Current Cleanliness

- Clean V1.5 worktree has no tracked diff after the latest package.
- Clean V1.5 staged diff is empty.
- Root `D:\gas_calibrator` staged diff is empty.
- Remaining untracked items in the clean worktree are `_handoff/...` historical evidence folders only.
- No COM, PostgreSQL, route-control, pressure-control, or coefficient-write process was started for this readiness note.

## Latest Code Package Before This Readiness Note

Latest commit:

```text
52ebe14d Refresh V1.5 formal flow inventory contract
```

Purpose:

- Use the current V1.5 entrypoint inventory automatically when exporting the formal-flow contract without an explicit inventory JSON.
- Refresh `v1_5_formal_flow_contract.json` / `.md` so stale `entrypoint_not_in_inventory` warnings are cleared.
- Update the entrypoint call map database wording to reflect the current PostgreSQL 18 dry-run / locked-import chain.

Latest focused verification:

```text
78 passed in 91.18s
```

Covered:

- `tests/test_v1_5_formal_flow_contract.py`
- `tests/test_v1_5_entrypoint_inventory.py`
- `tests/test_v1_5_full_flow_orchestration.py`

## Contract Status

Formal flow contract:

- `status=pass`
- `issues=0`
- `warnings=0`
- `not_real_acceptance_evidence=true`

Mature route contract:

- `status=pass`
- `blocker_count=0`
- Legacy CO2 formal route remains `45` points.
- Legacy H2O formal route remains `13` wet points.
- New-algorithm difference layer remains outside the mature route runner: `profile_fit_input_R0_contract_supplements_write_contract`.

New-algorithm profile/runlist review:

- Formal new-algorithm CO2 runlist preview has `47` points.
- Formal new-algorithm H2O runlist preview has `14` points.
- Queue handoff preflight is `ready_for_dry_run_queue_handoff_review`.
- `live_queue_execution_allowed=false`.

Final acceptance / status rollup:

- `overall_status=review_required`
- `current_stage=initialization_readiness`
- `formal_release_allowed=false`
- `database_import_allowed=false`
- `can_continue_physical_flow=false`

Database import chain:

- PostgreSQL 18 is represented as dry-run / preflight / authorization / command-contract / blocked-executor / controlled-executor-design evidence.
- `connects_postgresql=false`
- `real_import_execution_allowed=false`
- `database_written=false`
- `database_import_allowed=false`

## Protected Path Review

Current worktree diff for the mature protected paths is empty:

- `src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py`
- `src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py`
- `src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py`
- `src/gas_calibrator/workflow/runner.py`
- `src/gas_calibrator/devices/gas_analyzer.py`
- `configs/default_config.json`

The full ahead range does include earlier, isolated commits that touched some protected or live-adjacent files. These must be reviewed as their own committed packages, not treated as current unreviewed drift:

- `ff1e3bda Add V1.5 CHECK monitor and analyzer protocol support`
- `ef152c24 Add V1.5 CO2 queue temperature failure audit row`
- `3c611c03 Clamp V1.5 formal sampling analyzer command gap`

No full-ahead-range change was found for the mature H2O queue path in the protected-path check.

## Review Package Themes

The pre-readiness code commits are grouped into these review themes:

1. Initialization / identity / SN / PostgreSQL 18 pre-gas readiness.
2. CHECK monitor and 15-field analyzer protocol support.
3. Mature route contract, canonical entrypoint inventory, dirty-zone blockers, and final structure docs.
4. Historical replay contract, evidence binder, QC-gap audit, and missing-point audit.
5. New-algorithm profile guard: legacy `45/13`, new algorithm `47/14`, dry-run queue handoff only.
6. SENCOA/SENCOB, H2O, R0, CO2 S13/S5 write-contract and no-write review packages.
7. Formal run status, archive/release gates, and final acceptance status.
8. PostgreSQL 18 dry-run import chain, blocked executor, and controlled executor design.

## Push / Merge Recommendation

Ready to prepare a review push, with two boundaries:

1. This is ready for code review / merge review of the V1.5 structure-cleanup branch.
2. This is not ready for production release, live queue execution, formal database import, or real acceptance.

Before merging to a shared production branch, reviewers should explicitly acknowledge:

- Mature legacy routes remain protected by contract guards.
- New-algorithm route expansion is profile/dry-run only until a separate live-run wiring review.
- PostgreSQL 18 import remains locked and no-connect.
- `_handoff` evidence folders are not part of the clean package unless deliberately archived separately.
- Root `D:\gas_calibrator` remains a polluted draft surface and is not the authoritative V1.5 source.
