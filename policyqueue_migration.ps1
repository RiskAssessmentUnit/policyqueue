# Windows PolicyQueue Migration Script (Option 1)
# Purpose: Execute migration plan to make C:\Users\meta4\policyqueue canonical live pipeline host again.

$ErrorActionPreference = "Stop"

# =========================
# CONFIG
# =========================
$CanonicalRoot = "C:\Users\meta4\policyqueue"
$DestScripts   = Join-Path $CanonicalRoot "scripts\python"
$DestOps       = Join-Path $CanonicalRoot "ops"
$DestDaily     = Join-Path $DestOps "daily_health"
$DestPids      = Join-Path $CanonicalRoot "pids"
$ReportPath    = Join-Path $DestOps "windows_pipeline_migration_execution.md"

$SourceCandidates = @(
    "\\wsl$\Ubuntu\data\.openclaw\workspace\policyqueue",
    "\\wsl$\Ubuntu-22.04\data\.openclaw\workspace\policyqueue",
    "\\wsl.localhost\Ubuntu\data\.openclaw\workspace\policyqueue",
    "\\wsl.localhost\Ubuntu-22.04\data\.openclaw\workspace\policyqueue",
    "D:\data\.openclaw\workspace\policyqueue",
    "E:\data\.openclaw\workspace\policyqueue"
)

$RequiredRelativePaths = @(
    "scripts\python\continuous_runner.py",
    "ops\watcher_startup_check.json",
    "ops\daily_health\2026-03-08.json"
)

# =========================
# HELPERS
# =========================

function New-DirSafe {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
}

function Get-FileMeta {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    $i = Get-Item -LiteralPath $Path
    [pscustomobject]@{
        Path  = $i.FullName
        Size  = $i.Length
        MTime = $i.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss")
    }
}

function Stop-StaleOrRunningContinuousRunner {
    param([string]$CanonicalRootPath)

    $pidFile = Join-Path $CanonicalRootPath "pids\continuous_runner.pid"
    $stopped = @()

    if (Test-Path -LiteralPath $pidFile) {
        try {
            $raw = (Get-Content -LiteralPath $pidFile | Select-Object -First 1).Trim()
            if ($raw -match '^\d+$') {
                $oldPid = [int]$raw
                $proc = Get-Process -Id $oldPid -ErrorAction SilentlyContinue
                if ($proc) {
                    Stop-Process -Id $oldPid -Force
                    $stopped += "Stopped previous runner via PID file: $oldPid"
                }
            }
        } catch {}
    }

    $procs = Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine -match "continuous_runner\.py"
    }

    foreach ($p in $procs) {
        try {
            Stop-Process -Id $p.ProcessId -Force
            $stopped += "Stopped previous runner by process scan: $($p.ProcessId)"
        } catch {}
    }

    return $stopped
}

function Start-ContinuousRunner {
    param([string]$CanonicalRootPath)

    $runnerPy = Join-Path $CanonicalRootPath "scripts\python\continuous_runner.py"
    $outFile  = Join-Path $CanonicalRootPath "ops\continuous_runner.out"
    $pidFile  = Join-Path $CanonicalRootPath "pids\continuous_runner.pid"

    if (-not (Test-Path -LiteralPath $runnerPy)) {
        throw "continuous_runner.py not found at $runnerPy"
    }

    $pythonExe = $null

    try {
        if (Get-Command py -ErrorAction SilentlyContinue) {
            $pythonExe = "py"
        }
    } catch {}

    if (-not $pythonExe) {
        if (Get-Command python -ErrorAction SilentlyContinue) {
            $pythonExe = "python"
        }
    }

    if (-not $pythonExe) {
        throw "Python not found in PATH."
    }

    $args = if ($pythonExe -eq "py") { @("-3", $runnerPy) } else { @($runnerPy) }

    $p = Start-Process `
        -FilePath $pythonExe `
        -ArgumentList $args `
        -WorkingDirectory $CanonicalRootPath `
        -RedirectStandardOutput $outFile `
        -RedirectStandardError $outFile `
        -PassThru

    Start-Sleep -Seconds 3

    $runnerPid = $p.Id

    New-DirSafe (Split-Path $pidFile)
    Set-Content $pidFile $runnerPid

    $alive = $false
    try {
        if (Get-Process -Id $runnerPid -ErrorAction SilentlyContinue) {
            $alive = $true
        }
    } catch {}

    return [pscustomobject]@{
        RunnerPid = $runnerPid
        Alive     = $alive
        OutFile   = $outFile
        PidFile   = $pidFile
    }
}

# =========================
# SCRIPT START
# =========================

New-DirSafe $CanonicalRoot
New-DirSafe $DestScripts
New-DirSafe $DestOps
New-DirSafe $DestDaily
New-DirSafe $DestPids

$SourceRoot = $null

foreach ($c in $SourceCandidates) {
    if (Test-Path $c) {
        $SourceRoot = $c
        break
    }
}

if (-not $SourceRoot) {
    throw "Could not locate Linux workspace source."
}

foreach ($rel in $RequiredRelativePaths) {

    $src = Join-Path $SourceRoot $rel
    $dst = Join-Path $CanonicalRoot $rel

    if (Test-Path $src) {
        New-DirSafe (Split-Path $dst)
        Copy-Item $src $dst -Force
    }
}

Stop-StaleOrRunningContinuousRunner $CanonicalRoot

$runner = Start-ContinuousRunner $CanonicalRoot

Write-Host ""
Write-Host "Migration complete"
Write-Host "Runner PID:" $runner.RunnerPid
Write-Host "Runner Alive:" $runner.Alive
Write-Host "Log:" $runner.OutFile