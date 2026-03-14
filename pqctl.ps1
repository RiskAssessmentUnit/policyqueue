param(
  [Parameter(Position=0)] [string]$action = "status",
  [Parameter(Position=1)] [string]$svc = "all"
)

$ErrorActionPreference = "Stop"

$ROOT = Join-Path $HOME "policyqueue"
$PIDS = Join-Path $ROOT "pids"
$LOGS = Join-Path $ROOT "logs"

$RUNNER   = Join-Path $ROOT "runner.py"
$APPROVER = Join-Path $ROOT "approver.py"

New-Item -ItemType Directory -Force -Path $PIDS,$LOGS | Out-Null

function _pidPath([string]$name) { Join-Path $PIDS "$name.pid" }

function _isRunning([int]$procId) {
  try { Get-Process -Id $procId -ErrorAction Stop | Out-Null; return $true } catch { return $false }
}

function Stop-ServicePq([string]$name) {
  $pp = _pidPath $name
  if (-not (Test-Path $pp)) { Write-Host "OK: $name not running (no pid file)"; return }
  $procId = [int](Get-Content $pp -ErrorAction SilentlyContinue)
  if ($procId -and (_isRunning $procId)) {
    Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    Start-Sleep 1
    Write-Host "STOPPED: $name pid=$procId"
  } else {
    Write-Host "OK: $name not running (stale pid)"
  }
  Remove-Item $pp -Force -ErrorAction SilentlyContinue
}

function Start-ServicePq([string]$name) {
  if ($name -eq "runner") {
    $scriptPath = $RUNNER
  } elseif ($name -eq "approver") {
    $scriptPath = $APPROVER
  } else {
    throw "Unknown service: $name"
  }

  if (-not (Test-Path $scriptPath)) { Write-Host "ERR: missing $scriptPath"; return }

  $outLog = Join-Path $LOGS "$name.out.log"
  $errLog = Join-Path $LOGS "$name.err.log"
  $pidFile = _pidPath $name

  # already running?
  if (Test-Path $pidFile) {
    $procId = [int](Get-Content $pidFile -ErrorAction SilentlyContinue)
    if ($procId -and (_isRunning $procId)) { Write-Host "OK: $name already running pid=$procId"; return }
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
  }

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = "C:\Python39\python.exe"
  $psi.Arguments = "`"$scriptPath`""
  $psi.WorkingDirectory = $ROOT
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError  = $true
  $psi.UseShellExecute = $false
  $psi.CreateNoWindow = $true

  $p = New-Object System.Diagnostics.Process
  $p.StartInfo = $psi
  [void]$p.Start()

  Set-Content -Encoding ascii -Path $pidFile -Value $p.Id

  # stream output to files
  $stdOut = New-Object System.IO.StreamWriter($outLog, $true)
  $stdErr = New-Object System.IO.StreamWriter($errLog, $true)

  Register-ObjectEvent -InputObject $p -EventName OutputDataReceived -Action {
    if ($EventArgs.Data) { $stdOut.WriteLine($EventArgs.Data); $stdOut.Flush() }
  } | Out-Null
  Register-ObjectEvent -InputObject $p -EventName ErrorDataReceived -Action {
    if ($EventArgs.Data) { $stdErr.WriteLine($EventArgs.Data); $stdErr.Flush() }
  } | Out-Null

  $p.BeginOutputReadLine()
  $p.BeginErrorReadLine()

  Write-Host "STARTED: $name pid=$($p.Id) -> $name.out.log + $name.err.log"
}

function Status-Pq {
  Write-Host "=== PolicyQueue Processes ==="
  foreach ($name in @("runner","approver")) {
    $pp = _pidPath $name
    if (-not (Test-Path $pp)) { Write-Host ("DOWN  {0}" -f $name); continue }
    $procId = [int](Get-Content $pp -ErrorAction SilentlyContinue)
    if ($procId -and (_isRunning $procId)) { Write-Host ("UP    {0} pid={1}" -f $name, $procId) }
    else { Write-Host ("DOWN  {0} (stale pid)" -f $name) }
  }
}

switch ($action.ToLower()) {
  "start"   { if ($svc -eq "all") { Start-ServicePq runner; Start-ServicePq approver } else { Start-ServicePq $svc } }
  "stop"    { if ($svc -eq "all") { Stop-ServicePq runner; Stop-ServicePq approver } else { Stop-ServicePq $svc } }
  "restart" { if ($svc -eq "all") { Stop-ServicePq runner; Stop-ServicePq approver; Start-ServicePq runner; Start-ServicePq approver } else { Stop-ServicePq $svc; Start-ServicePq $svc } }
  "status"  { Status-Pq }
  "menu"    { Status-Pq; Write-Host "`nCommands:`n  start all|runner|approver`n  stop all|runner|approver`n  restart all|runner|approver`n  status`n" }
  default   { Status-Pq }
}
