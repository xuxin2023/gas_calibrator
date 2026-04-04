# Sync current branch changes to GitHub.
[CmdletBinding()]
param(
    [string]$Message,
    [switch]$DryRun,
    [switch]$SkipFetch
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-GitText {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [switch]$AllowFailure
    )

    $output = & git @Arguments 2>&1
    $exitCode = $LASTEXITCODE
    if (-not $AllowFailure -and $exitCode -ne 0) {
        throw "git $($Arguments -join ' ') failed.`n$output"
    }

    return (($output | Out-String).Trim())
}

function Invoke-GitWrite {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    Write-Host "> git $($Arguments -join ' ')"
    if ($DryRun) {
        return
    }

    $output = & git @Arguments 2>&1
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "git $($Arguments -join ' ') failed.`n$output"
    }

    if ($output) {
        $output | ForEach-Object { Write-Host $_ }
    }
}

function Test-GitRef {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RefName
    )

    & git show-ref --verify --quiet $RefName
    return ($LASTEXITCODE -eq 0)
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $repoRoot

try {
    $insideWorkTree = Get-GitText -Arguments @("rev-parse", "--is-inside-work-tree")
    if ($insideWorkTree -ne "true") {
        throw "The script must run inside a Git work tree."
    }

    $branch = Get-GitText -Arguments @("branch", "--show-current")
    if (-not $branch) {
        throw "Could not determine the current branch."
    }

    $originUrl = Get-GitText -Arguments @("remote", "get-url", "origin")
    if (-not $originUrl) {
        throw "Remote 'origin' is not configured."
    }

    if (-not $SkipFetch) {
        if ($DryRun) {
            Write-Host "Dry run: skipping 'git fetch'. Current origin refs will be used."
        }
        else {
            Invoke-GitWrite -Arguments @("fetch", "origin", $branch, "--quiet")
        }
    }

    $statusLines = @(git status --short)
    if ($LASTEXITCODE -ne 0) {
        throw "git status --short failed."
    }

    if ($statusLines.Count -eq 0) {
        Write-Host "No local changes to sync."
        return
    }

    Write-Host "Current branch: $branch"
    Write-Host "Remote: $originUrl"
    Write-Host "Pending changes:"
    $statusLines | ForEach-Object { Write-Host "  $_" }

    if (Test-GitRef -RefName "refs/remotes/origin/$branch") {
        $aheadBehind = Get-GitText -Arguments @("rev-list", "--left-right", "--count", "HEAD...origin/$branch")
        $parts = $aheadBehind -split "\s+"
        if ($parts.Count -ge 2) {
            $remoteOnly = [int]$parts[1]
            if ($remoteOnly -gt 0) {
                throw "origin/$branch has $remoteOnly commit(s) that are not in local $branch. Pull or rebase first, then re-run the sync."
            }
        }
    }

    if (-not $Message) {
        $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        $Message = "chore: sync $timestamp"
    }

    Write-Host "Commit message: $Message"
    if ($DryRun) {
        $addPreview = Get-GitText -Arguments @("add", "-A", "--dry-run")
        if ($addPreview) {
            Write-Host "Dry-run add preview:"
            $addPreview -split "`r?`n" | Where-Object { $_ } | ForEach-Object { Write-Host "  $_" }
        }

        $upstream = Get-GitText -Arguments @("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}") -AllowFailure
        Write-Host "> git commit -m `"$Message`""
        if ($upstream) {
            Write-Host "> git push"
        }
        else {
            Write-Host "> git push -u origin $branch"
        }
        Write-Host "Dry run complete. No commit or push was performed."
        return
    }

    Invoke-GitWrite -Arguments @("add", "-A")

    & git diff --cached --quiet
    switch ($LASTEXITCODE) {
        0 {
            Write-Host "No staged changes to commit."
            return
        }
        1 { }
        default {
            throw "git diff --cached --quiet failed."
        }
    }

    Invoke-GitWrite -Arguments @("commit", "-m", $Message)

    $upstream = Get-GitText -Arguments @("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}") -AllowFailure
    if ($upstream) {
        Invoke-GitWrite -Arguments @("push")
    }
    else {
        Invoke-GitWrite -Arguments @("push", "-u", "origin", $branch)
    }

    Write-Host "Sync complete."
}
finally {
    Pop-Location
}
