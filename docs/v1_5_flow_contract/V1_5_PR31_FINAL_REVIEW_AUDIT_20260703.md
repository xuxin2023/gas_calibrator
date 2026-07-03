# V1.5 PR #31 Final Review Audit - 2026-07-03

This note is a reviewer-facing audit for PR #31. It is not production release
evidence and does not authorize live queue execution, PostgreSQL import,
coefficient writes, or real acceptance.

## PR Status

- PR: https://github.com/xuxin2023/gas_calibrator/pull/31
- Branch: `codex/v1.5-structure-review-clean-main`
- Base: `main`
- Reviewed source head before adding this audit note:
  `2c04b8a068f531a50c74e257e6ed0713b4e1e339`
- Current PR head should be read from GitHub after this note, because the audit
  note itself is committed as a doc-only follow-up.
- GitHub PR state: `open`
- GitHub draft state: `true`
- GitHub mergeable: `true`
- GitHub mergeable state: `clean`
- GitHub check-runs returned by API: `0`
- GitHub failed check-runs returned by API: `0`
- GitHub combined status: `pending` because no status contexts were returned.

## Why PR #31 Replaces PR #30

PR #30 was based on the earlier V1.5 structure branch and conflicted with the
current `main` branch, especially around live-adjacent implementation files.
PR #31 was rebuilt from the latest `origin/main` so that the V1.5 structure,
contract, replay, profile, and database-lock packages can be reviewed without
merge conflicts.

Important differences from PR #30:

- PR #31 starts from latest `main`.
- PR #31 is mergeable and has no base-branch conflicts at audit time.
- PR #31 removed `_handoff` artifacts from the review diff.
- PR #31 does not include the CHECK / runner live-adjacent conflict package.
- PR #31 does not modify:
  - `src/gas_calibrator/workflow/runner.py`
  - `src/gas_calibrator/devices/gas_analyzer.py`
  - `configs/default_config.json`
  - `run_app.py`

## Diff Boundary

At audit capture time, `origin/main..HEAD` contained `634` changed files before
this audit note was added. After this note is committed, the current PR diff is
expected to contain one additional documentation file.

That large number is expected for a V1.5 structure backfill, but reviewers
should not read all files as production behavior changes. The diff is grouped
into review themes:

1. V1.5 structure, flow-contract, entrypoint inventory, and guard documents.
2. Mature-route contract and historical replay / evidence-binder guards.
3. Legacy/new algorithm profile contracts, including legacy `45/13` and new
   algorithm `47/14` point plans.
4. Algorithm runlist preview, readiness, profile-runner dry-run, and dry-run
   queue-handoff preflight.
5. PostgreSQL 18 dry-run import contract, blocked executor, and controlled
   executor design. These remain no-connect and no-write.
6. Formal run status, archive/release lock gates, operation console, and final
   acceptance rollups.
7. V1.5 formal queue and validation support file backfill.
8. Tests and generated reviewer-facing evidence under `docs/`.

## Handoff Artifact Decision

Earlier review of PR #31 found `23` `_handoff` files in the PR diff. These were
old handoff/evidence artifacts, not required by the formal source, tests,
configs, or docs. They were removed from PR #31 to keep the review package
aligned with the rule that `_handoff` is not part of the formal package.

Current `_handoff` diff count:

```text
0
```

## Protected Path Review

The following high-risk core files are unchanged in PR #31:

```text
src/gas_calibrator/workflow/runner.py
src/gas_calibrator/devices/gas_analyzer.py
configs/default_config.json
run_app.py
```

This is intentional. PR #31 is a structure and contract review PR, not a live
runner/protocol/default-entry rewrite.

## Current Lock State

The V1.5 status contracts remain locked:

- `formal_release_allowed=false`
- `database_import_allowed=false`
- `can_continue_physical_flow=false`
- `live_queue_execution_allowed=false`
- `connects_postgresql=false`
- `database_written=false`

New-algorithm queue handoff remains dry-run review only:

- `dry_run_handoff_review_allowed=true`
- `live_queue_execution_allowed=false`

PostgreSQL import remains design / blocked / no-connect:

- `production_state=blocked_design_only`
- `execution_supported=false`
- `connects_postgresql=false`
- `database_written=false`

## Verification

Local verification after removing `_handoff` artifacts:

```text
python -m pytest tests/test_v1_5_entrypoint_inventory.py tests/test_v1_5_formal_flow_contract.py tests/test_v1_5_full_flow_orchestration.py tests/test_v1_5_mature_route_contract.py tests/test_v1_5_algorithm_route_profiles.py tests/test_v1_5_formal_run_status.py tests/test_v1_5_formal_database_import_command_contract.py tests/test_v1_5_pressure_channel_validation.py tests/test_validation_tools.py tests/test_v1_5_calibratable_point_policy.py tests/test_v1_5_calibration_capability.py tests/test_v1_5_canonical_evidence_package.py tests/test_v1_5_formal_evidence_run.py tests/test_v1_5_formal_contracts_preflight.py -q
```

Result:

```text
176 passed, 3 warnings in 51.54s
```

The warnings are existing unregistered pytest mark warnings for
`v1_5_formal_gate`; they are not introduced by the `_handoff` removal.

Additional checks:

```text
git diff --check origin/main..HEAD
git diff --name-only origin/main..HEAD -- _handoff
git diff --name-only origin/main..HEAD -- src/gas_calibrator/workflow/runner.py src/gas_calibrator/devices/gas_analyzer.py configs/default_config.json run_app.py
```

Results:

- Diff whitespace check passed.
- `_handoff` diff is empty.
- Protected high-risk core-file diff is empty.

## Review Recommendation

PR #31 is ready to continue as a clean, draft review PR. It should not be
treated as production release readiness.

Reviewer focus:

1. Confirm the V1.5 structure and contract boundaries are acceptable.
2. Confirm the new-algorithm `47/14` profile remains dry-run / handoff-only.
3. Confirm PostgreSQL 18 import remains no-connect / no-write.
4. Confirm V1.5 formal queue backfill is acceptable as V1.5-specific support
   and not a modification of mainline `runner.py`.
5. Confirm no `_handoff` artifacts are part of the PR.

If GitHub checks are later added and pass, this PR can be converted from draft
to ready for review. That conversion should still be understood as code review
readiness, not production release or live execution authorization.
