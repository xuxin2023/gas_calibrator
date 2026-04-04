param(
    [string]$RunId,
    [Parameter(Mandatory = $true)][string]$RunDir,
    [string]$StdoutPath,
    [int]$TargetPid = 0
)

$ErrorActionPreference = 'SilentlyContinue'
$RunDir = [System.IO.Path]::GetFullPath($RunDir)
if ([string]::IsNullOrWhiteSpace($RunId)) {
    $RunId = Split-Path -Path $RunDir -Leaf
}
if ([string]::IsNullOrWhiteSpace($StdoutPath)) {
    $candidateInRunDir = Join-Path $RunDir ($RunId + '_stdout.log')
    $candidateInParent = Join-Path (Split-Path -Path $RunDir -Parent) ($RunId + '_stdout.log')
    if (Test-Path $candidateInRunDir) {
        $StdoutPath = $candidateInRunDir
    } else {
        $StdoutPath = $candidateInParent
    }
}
$Host.UI.RawUI.WindowTitle = 'Gas Calibrator 8 Analyzer Monitor - ' + $RunId

function Get-LatestIoFile {
    Get-ChildItem -Path $RunDir -Filter 'io_*.csv' -File |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
}

function Get-AnalyzerRows {
    param([string]$IoPath)

    $labels = 1..8 | ForEach-Object { 'ga{0:d2}' -f $_ }
    $rows = @()
    $dropMap = @{}

    if (Test-Path $StdoutPath) {
        $dropLine = Get-Content -Path $StdoutPath | Select-String -Pattern 'Analyzers dropped from active set:' | Select-Object -Last 1
        if ($dropLine) {
            $m = [regex]::Match($dropLine.Line, 'Analyzers dropped from active set:\s*([^\r\n]+?)\s+reason=')
            if ($m.Success) {
                foreach ($name in ($m.Groups[1].Value -split ',\s*')) {
                    if ($name) { $dropMap[$name.Trim()] = 'DROPPED' }
                }
            }
        }
    }

    $ioRows = @()
    if (Test-Path $IoPath) {
        $ioRows = Import-Csv -Path $IoPath
    }

    foreach ($label in $labels) {
        $deviceRows = @($ioRows | Where-Object { $_.device -eq $label } | Select-Object -Last 6)
        $lastRx = $deviceRows | Where-Object { $_.direction -eq 'RX' } | Select-Object -Last 1
        $lastTx = $deviceRows | Where-Object { $_.direction -eq 'TX' } | Select-Object -Last 1

        $status = 'ACTIVE'
        if ($dropMap.ContainsKey($label)) {
            $status = 'DROPPED'
        } elseif (-not $lastRx -and -not $lastTx) {
            $status = 'NO_DATA'
        }

        $summary = ''
        if ($lastRx) {
            $summary = $lastRx.response
        } elseif ($lastTx) {
            $summary = 'TX ' + $lastTx.command
        }
        if ($summary.Length -gt 100) {
            $summary = $summary.Substring(0, 100)
        }

        $rows += [pscustomobject]@{
            Analyzer = $label
            Status   = $status
            Time     = if ($lastRx) { $lastRx.timestamp } elseif ($lastTx) { $lastTx.timestamp } else { '' }
            Summary  = $summary
        }
    }

    return $rows
}

while ($true) {
    Clear-Host
    Write-Host ('Run: ' + $RunId)
    if ($TargetPid -gt 0) {
        $proc = Get-Process -Id $TargetPid -ErrorAction SilentlyContinue
        if ($proc) {
            Write-Host ('Process: RUNNING  PID=' + $TargetPid)
        } else {
            Write-Host ('Process: STOPPED  PID=' + $TargetPid)
        }
    }
    Write-Host ('Time: ' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))
    Write-Host ''

    Write-Host '--- Recent Stage ---'
    if (Test-Path $StdoutPath) {
        Get-Content -Path $StdoutPath -Tail 20
    } else {
        Write-Host 'stdout log not ready'
    }

    Write-Host ''
    Write-Host '--- Analyzer Status ---'
    $io = Get-LatestIoFile
    if ($io) {
        Get-AnalyzerRows -IoPath $io.FullName | Format-Table -AutoSize | Out-String -Width 240 | Write-Host
    } else {
        Write-Host 'io log not ready'
    }

    Start-Sleep -Seconds 2
}
