# pq_fixall2.ps1
# One-shot: stop PQ processes, fix Windows lock bug in runner_focus_v4.py,
# make approver archive sent posts (no more disappearing), clean stale pid files,
# compile-check.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ROOT = (Get-Location).Path
if (!(Test-Path ".\runner_focus_v4.py")) { throw "Run this from C:\Users\meta4\policyqueue (runner_focus_v4.py not found)." }
if (!(Test-Path ".\approver.py"))        { throw "approver.py not found in this folder." }

function Backup-File($path) {
  if (Test-Path $path) {
    $ts = Get-Date -Format "yyyyMMdd-HHmmss"
    Copy-Item $path "$path.bak.$ts" -Force
  }
}

function Stop-PythonByMatch([string]$pattern) {
  $procs = Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -and ($_.CommandLine -match $pattern)
  }
  foreach ($p in $procs) {
    try {
      Write-Host "Stopping PID $($p.ProcessId): $($p.CommandLine)"
      Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    } catch {}
  }
}

Write-Host "`n== 1) Stop PQ-related processes ==" -ForegroundColor Cyan
Stop-PythonByMatch "runner_focus_v4\.py"
Stop-PythonByMatch "approver\.py"
Stop-PythonByMatch "discord_notifier\.py"

Write-Host "`n== 2) Remove stale PID files ==" -ForegroundColor Cyan
if (Test-Path ".\pids") {
  Get-ChildItem ".\pids" -Filter "*.pid" -ErrorAction SilentlyContinue | ForEach-Object {
    Write-Host "Deleting PID file: $($_.FullName)"
    Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
  }
}

Write-Host "`n== 3) Patch runner_focus_v4.py (Windows-safe PID check) ==" -ForegroundColor Cyan
$runnerPath = ".\runner_focus_v4.py"
Backup-File $runnerPath
$runner = Get-Content $runnerPath -Raw -Encoding UTF8

# We patch the exact problematic behavior: os.kill(pid, 0) on Windows can throw WinError 87.
# Replace any line that looks like os.kill(int(old), 0) or os.kill(old, 0) with a cross-platform check.
$patchBlock = @'
if os.name == "nt":
            import subprocess
            r = subprocess.run(["tasklist", "/FI", f"PID eq {int(old)}"], capture_output=True, text=True)
            if str(int(old)) in (r.stdout or ""):
                raise SystemExit(f"Runner already running (pid {old}).")
else:
            os.kill(int(old), 0)
'@

$didRunnerPatch = $false

# Case A: exact "os.kill(int(old), 0)"
if ($runner -match 'os\.kill\(\s*int\(\s*old\s*\)\s*,\s*0\s*\)') {
  $runner = [regex]::Replace(
    $runner,
    '(?m)^\s*os\.kill\(\s*int\(\s*old\s*\)\s*,\s*0\s*\)\s*$',
    $patchBlock
  )
  $didRunnerPatch = $true
}

# Case B: "os.kill(old, 0)" (some variants)
if (-not $didRunnerPatch -and $runner -match 'os\.kill\(\s*old\s*,\s*0\s*\)') {
  $runner = [regex]::Replace(
    $runner,
    '(?m)^\s*os\.kill\(\s*old\s*,\s*0\s*\)\s*$',
    @'
if os.name == "nt":
            import subprocess
            r = subprocess.run(["tasklist", "/FI", f"PID eq {int(old)}"], capture_output=True, text=True)
            if str(int(old)) in (r.stdout or ""):
                raise SystemExit(f"Runner already running (pid {old}).")
else:
            os.kill(old, 0)
'@
  )
  $didRunnerPatch = $true
}

if (-not $didRunnerPatch) {
  throw "Could not find the Windows-breaking os.kill(...,0) line in runner_focus_v4.py. Search it: Select-String runner_focus_v4.py -Pattern 'os.kill' -Context 2,2"
}

[System.IO.File]::WriteAllText((Resolve-Path $runnerPath), $runner, [System.Text.UTF8Encoding]::new($false))
Write-Host "Patched runner_focus_v4.py (Windows-safe PID check)."

Write-Host "`n== 4) Patch approver.py (archive sent posts instead of deleting) ==" -ForegroundColor Cyan
$approverPath = ".\approver.py"
Backup-File $approverPath
$approver = Get-Content $approverPath -Raw -Encoding UTF8

# Ensure SENT_DIR exists
if ($approver -notmatch '(?m)^SENT_DIR\s*=') {
  $approver = $approver -replace '(?m)^(ROOT\s*=\s*Path\.home\(\)\s*/\s*"policyqueue"\s*)\s*$',
@'
$1
SENT_DIR = ROOT / "sent"
'@
}

# Ensure SENT_DIR.mkdir exists near QUEUE.mkdir
if ($approver -match '(?m)^\s*QUEUE\.mkdir\(parents=True,\s*exist_ok=True\)\s*$' -and $approver -notmatch 'SENT_DIR\.mkdir') {
  $approver = $approver -replace '(?m)^\s*QUEUE\.mkdir\(parents=True,\s*exist_ok=True\)\s*$',
@'
    QUEUE.mkdir(parents=True, exist_ok=True)
    SENT_DIR.mkdir(parents=True, exist_ok=True)
'@
}

# Replace the post-send unlink with move-to-sent logic.
# This targets the specific block "remove from queue once sent" OR any unlink right after save_json.
$approver = $approver -replace '(?ms)#\s*remove from queue once sent\s*\r?\n\s*p\.unlink\(missing_ok=True\)',
@'
# archive sent post (so files don't "disappear")
try:
    dst = SENT_DIR / p.name
    if dst.exists():
        import time
        dst = SENT_DIR / f"{p.stem}.{int(time.time())}{p.suffix}"
    p.replace(dst)
except Exception:
    # last resort: delete from queue to prevent resend loops
    p.unlink(missing_ok=True)
'@

if ($approver -notmatch 'archive sent post') {
  $approver = $approver -replace '(?ms)(save_json\(SENT,\s*sent\)\s*\r?\n\s*)(p\.unlink\(missing_ok=True\))',
@'
\1# archive sent post (so files don't "disappear")
                try:
                    dst = SENT_DIR / p.name
                    if dst.exists():
                        import time
                        dst = SENT_DIR / f"{p.stem}.{int(time.time())}{p.suffix}"
                    p.replace(dst)
                except Exception:
                    p.unlink(missing_ok=True)
'@
}

[System.IO.File]::WriteAllText((Resolve-Path $approverPath), $approver, [System.Text.UTF8Encoding]::new($false))
Write-Host "Patched approver.py to move sent posts into .\sent\"

Write-Host "`n== 5) Ensure folders exist ==" -ForegroundColor Cyan
$folders = @("queue","sent","logs","state","pids")
foreach ($f in $folders) { New-Item -ItemType Directory -Path (Join-Path $ROOT $f) -Force | Out-Null }

Write-Host "`n== 6) Compile-check Python files ==" -ForegroundColor Cyan
py -c "import py_compile; py_compile.compile(r'runner_focus_v4.py', doraise=True); py_compile.compile(r'approver.py', doraise=True); print('OK: runner_focus_v4.py and approver.py compile')"

Write-Host "`nDONE." -ForegroundColor Green
Write-Host "Next:"
Write-Host "  Start runner:   py .\runner_focus_v4.py loop"
Write-Host "  Start approver: py .\approver.py"
Write-Host "Sent posts will now be kept in: .\sent\ (instead of being deleted)."