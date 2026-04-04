param(
    [string]$RunDir = "",
    [string]$Config = "configs/default_config.json"
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = Join-Path $repoRoot "src"

$argsList = @("-m", "gas_calibrator.tools.audit_run")
if ($RunDir) {
    $argsList += @("--run-dir", $RunDir)
}
if ($Config) {
    $argsList += @("--config", $Config)
}

python @argsList
exit $LASTEXITCODE
