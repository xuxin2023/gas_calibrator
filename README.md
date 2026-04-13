# Gas Analyzer Auto-Calibration

This repository contains the gas analyzer auto-calibration codebase.

## Current Phase

The authoritative project scope is:

1. `AGENTS.md`
2. `.ai-context/PROJECT_STATUS.md`
3. `.ai-context/TODO.md`

The project is currently in `Step 2: production-grade platformization`.
`Step 3` work is out of scope for the current default workflow.

## Step 2 Defaults

- V2 is the active development line.
- V1 stays frozen as the production baseline and historical reference.
- `run_app.py` remains the V1 default entry and must not be changed as part of Step 2 work.
- V2 validation is limited to:
  - `simulation`
  - `replay`
  - `suite regression`
  - `parity`
  - `resilience`
  - `offline review`
  - `UI contract`

## Step 2 Prohibited Paths

The following are not part of the current default workflow:

- real device bring-up
- hardware trial runs
- opening real serial or COM ports
- `real compare` or `real verify`
- refreshing `real_primary_latest`
- claiming V2 can replace V1
- switching the default entrypoint to V2

Any simulated, replay, suite, parity, resilience, workbench, or offline-review result is not real acceptance evidence.

## Recommended Entrypoints

### V2 safe simulation

```powershell
PYTHONPATH=src python -m gas_calibrator.v2.scripts.test_v2_safe
```

```powershell
PYTHONPATH=src python -m gas_calibrator.v2.scripts.run_v2 --config src/gas_calibrator/v2/configs/smoke_v2_minimal.json --simulation --headless
```

### V2 device helper

`test_v2_device` is now a simulation-only helper by default. Its default path must not import or open real device drivers.

```powershell
PYTHONPATH=src python -m gas_calibrator.v2.scripts.test_v2_device connection
PYTHONPATH=src python -m gas_calibrator.v2.scripts.test_v2_device single
PYTHONPATH=src python -m gas_calibrator.v2.scripts.test_v2_device full
```

Any future bench-oriented real path is non-default and must stay explicitly locked behind:

- an explicit CLI unlock flag
- an explicit environment variable unlock
- a Step 2 warning in documentation

That future path is not part of the current recommended workflow.

### V1 historical reference

```powershell
python run_app.py
```

Use V1 only as the frozen baseline, historical reference, or emergency fallback. Do not route new V2 capabilities back into the V1 UI.

### V1 safe sidecars

```powershell
python run_v1_postprocess.py
```

```powershell
python run_v1_merged_sidecar.py --run-dir <completed_run_dir>
```

These entrypoints keep V1-adjacent postprocess/sidecar flows easy to reach
without changing `run_app.py`, without modifying the frozen V1 UI, and without
promoting Step 2 sidecar capabilities into the V1 production path.

## Offline Modeling

For offline ratio-poly modeling, `Validation` and `Test` are strict holdout
sets. The current holdout-safe behavior is:

- split the dataset before outlier filtering
- run outlier detection on `train` only
- fit original coefficients on `train` only
- choose simplification digits on `train` by default, or on `train+validation`
  only when explicitly configured
- keep `validation` and `test` for evaluation only

When point-level grouping metadata such as `PointTag` / `PointRow` /
`PointPhase` is present, offline modeling prefers group-aware splitting and
falls back to random splitting otherwise.

### GitHub sync

```powershell
.\scripts\sync.ps1
```

```powershell
.\scripts\sync.ps1 -Message "feat: describe your change"
```

The sync script stages the current branch changes, creates a commit, and pushes
to `origin`. Use `-DryRun` to preview the actions first. If `origin/<branch>`
is ahead of your local branch, the script stops and asks you to sync the branch
history first.

### Auto sync

You can register a Windows Scheduled Task that runs every 5 minutes and calls:

```powershell
.\scripts\run_auto_sync.ps1
```

This wrapper writes each unattended run to `logs/auto_sync/`.
