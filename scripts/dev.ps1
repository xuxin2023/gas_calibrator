# Start app in dev mode
$env:PYTHONPATH = "$PSScriptRoot\..\src"

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    $pythonCmd = Get-Command py -ErrorAction SilentlyContinue
}
if (-not $pythonCmd) {
    throw "Neither 'python' nor 'py' was found in PATH."
}

& $pythonCmd.Source "$PSScriptRoot\..\run_app.py"
