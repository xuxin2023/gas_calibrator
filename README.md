# Gas Analyzer Auto-Calibration

This repository contains the gas analyzer auto-calibration codebase.

## Current Product Direction

The authoritative project scope is:

1. `AGENTS.md`
2. `docs/architecture/V1_5_FINAL_PRODUCT_ARCHITECTURE_20260728.md`
3. `docs/architecture/V1_5_OPERATOR_WORKSTATION_DECISION_20260727.md`

V1.5 is the only final product line and the only target for future product work.
V1 remains the frozen production fallback and historical behavior baseline.
V2 is a migration/deletion pool, not a product version or launch target.

The default workflow remains no-write and does not open real COM ports, control
gas routes, write analyzer coefficients/IDs, update the formal database, or
refresh `real_primary_latest`. Simulation, replay, parity, resilience, and
dry-run evidence are never real acceptance evidence.

## Recommended Entrypoints

### V1.5 product candidate

```powershell
python run_v1_5_workstation.py
```

The V1.5 workstation currently exposes a governed dry-run path. Real queue
execution remains blocked until its separate acceptance and release gates are
closed.

### V1 production fallback

```powershell
python run_app.py
```

Use V1 only as the frozen baseline, historical reference, or emergency
fallback. Do not route V1.5 capabilities back into the V1 UI.

### Legacy V1 offline audit sidecar

```powershell
python run_v1_merged_sidecar.py --run-dir <completed_run_dir>
```

This entrypoint keeps the retained V1 merged-run audit sidecar reachable
without changing `run_app.py` or the frozen V1 UI. The obsolete
`run_v1_postprocess.py` GUI was retired because it depended on a removed V2
interface and exposed coefficient download as its default action.

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
