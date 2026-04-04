param(
    [string]$Config = "configs/default_config.json",
    [double]$Temp = 20.0,
    [string]$SkipCo2Ppm = "100,200,300,500,600,700,800,900",
    [switch]$EnableConnectCheck,
    [string]$RunId = ""
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = Join-Path $repoRoot "src"

$argsList = @(
    "-m", "gas_calibrator.tools.verify_short_run",
    "--config", $Config,
    "--temp", $Temp,
    "--skip-co2-ppm", $SkipCo2Ppm
)
if ($EnableConnectCheck) {
    $argsList += "--enable-connect-check"
}
if ($RunId) {
    $argsList += @("--run-id", $RunId)
}

python @argsList
exit $LASTEXITCODE
