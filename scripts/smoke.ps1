# Minimal smoke run (non-destructive)
$env:PYTHONPATH = "$PSScriptRoot\..\src"

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    $pythonCmd = Get-Command py -ErrorAction SilentlyContinue
}
if (-not $pythonCmd) {
    throw "Neither 'python' nor 'py' was found in PATH."
}

& $pythonCmd.Source -c "import sys; print('PYTHONPATH OK')"
& $pythonCmd.Source -c "import gas_calibrator; print('import OK')"
