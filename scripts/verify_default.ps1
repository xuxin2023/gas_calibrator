param(
    [string]$Config = "configs/default_config.json",
    [double]$Temp = 20.0,
    [string]$SkipCo2Ppm = "100,200,300,500,600,700,800,900",
    [switch]$EnableConnectCheck,
    [string]$RunId = ""
)

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$env:PYTHONPATH = Join-Path $repoRoot "src"

Write-Host "[1/3] python -m pytest -q"
python -m pytest -q
if ($LASTEXITCODE -ne 0) {
    Write-Host "verify_default.ps1 stopped: pytest failed with exit code $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "[2/3] smoke.ps1"
powershell -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "smoke.ps1")
if ($LASTEXITCODE -ne 0) {
    Write-Host "verify_default.ps1 stopped: smoke failed with exit code $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "[3/3] verify_short_run.ps1"
$shortArgs = @{
    Config = $Config
    Temp = $Temp
    SkipCo2Ppm = $SkipCo2Ppm
}
if ($EnableConnectCheck) {
    $shortArgs["EnableConnectCheck"] = $true
}
if ($RunId) {
    $shortArgs["RunId"] = $RunId
}

powershell -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "verify_short_run.ps1") @shortArgs
if ($LASTEXITCODE -ne 0) {
    Write-Host "verify_default.ps1 stopped: short verification failed with exit code $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "verify_default.ps1 finished successfully." -ForegroundColor Green
exit 0
