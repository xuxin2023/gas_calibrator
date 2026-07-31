[CmdletBinding()]
param(
    [ValidateSet("quick", "release", "nightly")]
    [string]$Tier = "quick",
    [string]$OutputDir = "",
    [string]$SourceOriginMainCommit = "",
    [ValidateRange(1, 86400)]
    [int]$TimeoutSeconds = 1800,
    [switch]$List
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$env:PYTHONPATH = Join-Path $repoRoot "src"
$env:PYTHONIOENCODING = "utf-8"

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    $pythonCmd = Get-Command py -ErrorAction SilentlyContinue
}
if (-not $pythonCmd) {
    throw "Neither 'python' nor 'py' was found in PATH."
}

$quickTestFiles = @(
    "tests/test_v1_5_operator_workstation.py",
    "tests/test_v1_5_operator_workstation_ui.py",
    "tests/test_v1_5_runtime_serial_port_binding.py",
    "tests/test_v1_5_mature_route_contract.py",
    "tests/test_v1_5_full_flow_orchestration.py",
    "tests/test_v1_5_legacy_full_flow_offline_replay.py",
    "tests/test_v1_5_historical_frame_parity_audit.py",
    "tests/test_v1_5_final_product_boundary.py"
)

$tierDescriptions = @{
    quick = "V1.5 current-change gate: exact critical-path pytest allowlist plus parity."
    release = "V1.5 formal offline release gate: existing 28-file acceptance runner plus parity."
    nightly = "V2 heavy simulation matrix: existing nightly suite; simulated evidence only."
}

if ($List) {
    foreach ($name in @("quick", "release", "nightly")) {
        Write-Output ("{0}: {1}" -f $name, $tierDescriptions[$name])
    }
    exit 0
}

$forbiddenEnvironmentVariables = @(
    "ALLOW_REAL_DEVICE_WRITE",
    "GAS_CAL_ALLOW_REAL_DEVICE_WRITE",
    "GAS_CAL_V2_QUERY_ONLY_REAL_COM",
    "GAS_CAL_DB_DSN",
    "V1_5_POSTGRES_DSN",
    "V1_5_POSTGRES_STAGING_DSN",
    "V1_5_POSTGRES_STAGING_DSN_TEST"
)
foreach ($name in $forbiddenEnvironmentVariables) {
    $value = [Environment]::GetEnvironmentVariable($name)
    if (-not [string]::IsNullOrWhiteSpace($value)) {
        throw "Offline test gate blocked because forbidden environment variable is set: $name"
    }
}

Set-Location $repoRoot
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $OutputDir = Join-Path $repoRoot ("_runtime\test_gates\{0}_{1}" -f $Tier, $timestamp)
}

function Invoke-Python {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)
    & $pythonCmd.Source @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Test tier '$Tier' failed with exit code $LASTEXITCODE."
    }
}

Write-Host ("Tier: {0}" -f $Tier)
Write-Host $tierDescriptions[$Tier]
Write-Host ("Output: {0}" -f $OutputDir)

switch ($Tier) {
    "quick" {
        Invoke-Python -Arguments (@("-m", "pytest", "-q") + $quickTestFiles)
        Invoke-Python -Arguments @(
            "-m",
            "gas_calibrator.v2.scripts.run_simulation_suite",
            "--suite",
            "parity",
            "--report-root",
            $OutputDir,
            "--run-name",
            "v1_5_quick_parity"
        )
    }
    "release" {
        if ($SourceOriginMainCommit -notmatch "^[0-9a-fA-F]{40}$") {
            throw "Release tier requires -SourceOriginMainCommit with an exact 40-character commit SHA."
        }
        Invoke-Python -Arguments @(
            "-m",
            "gas_calibrator.tools.run_v1_5_final_offline_acceptance_suite",
            "--repository-root",
            $repoRoot,
            "--source-origin-main-commit",
            $SourceOriginMainCommit.ToLowerInvariant(),
            "--output-dir",
            $OutputDir,
            "--timeout-s",
            [string]$TimeoutSeconds
        )
        Invoke-Python -Arguments @(
            "-m",
            "gas_calibrator.v2.scripts.run_simulation_suite",
            "--suite",
            "parity",
            "--report-root",
            $OutputDir,
            "--run-name",
            "v1_5_release_parity"
        )
    }
    "nightly" {
        Invoke-Python -Arguments @(
            "-m",
            "gas_calibrator.v2.scripts.run_simulation_suite",
            "--suite",
            "nightly",
            "--report-root",
            $OutputDir,
            "--run-name",
            "v2_nightly"
        )
    }
}

Write-Host ("Test tier '{0}' passed." -f $Tier) -ForegroundColor Green
exit 0
