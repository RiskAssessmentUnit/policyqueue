# pq_fixall.ps1
# One-shot: stop PQ processes, fix Windows lock bug, make approver archive sent posts (no more disappearing),
# clean stale pid files, compile-check.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ROOT = (Get-Location).Path
if (!(Test-Path ".\runner_focus_v4.py")) {
  throw "Run this from C:\Users\meta4\policyqueue (runner_focus_v4.py not found here)."
}

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

Write-Host "`n== 3) Patch runner_focus_v4.py (Windows-safe lock) ==" -ForegroundColor Cyan
$runnerPath = ".\runner_focus_v4.py"
Backup-File $runnerPath
$runner = Get-Content $runnerPath -Raw -Encoding UTF8

# Replace acquire_lock() function entirely with a Windows-safe implementation.
# This avoids os.kill(pid,0) on Windows and uses tasklist instead.
$acqPattern = '(?s)^def\s+acquire_lock\(\)\s*:\s*.*?(?=^\s*def\s+|\Z)'
$acqReplace = @'
def acquire_lock():
    """
    Cross-platform single-instance lock.
    - On Windows, os.kill(pid,0) can throw WinError 87; use tasklist instead.
    - Stores PID in pids/runner_focus_v4.pid
    """
    import os, time, subprocess
    from pathlib import Path

    ROOT = Path.home() / "policyqueue"
    PIDS = ROOT / "pids"
    PIDS.mkdir(parents=True, exist_ok=True)
    pidfile = PIDS / "runner_focus_v4.pid"

    def pid_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        if os.name == "nt":
            try:
                r = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}"],
                    capture_output=True, text=True, check=False
                )
                return str(pid) in (r.stdout or "")
            except Exception:
                return False
        else:
            try:
                os.kill(pid, 0)
                return True
            except OSError:
                return False

    if pidfile.exists():
        old = (pidfile.read_text(encoding="utf-8", errors="ignore") or "").strip()
        if old.isdigit() and pid_alive(int(old)):
            raise SystemExit(f"Runner already running (pid {old}). Delete {pidfile} if this is wrong.")
        # stale pidfile
        try:
            pidfile.unlink()
        except Exception:
            pass

    pidfile.write_text(str(os.getpid()), encoding="utf-8")
'@

if ($runner -match $acqPattern) {
  $runner2 = [regex]::Replace($runner, $acqPattern, $acqReplace, [System.Text.RegularExpressions.RegexOptions]::Multiline)
  if ($runner2 -eq $runner) { throw "Runner patch did not change file (unexpected)." }
  [System.IO.File]::WriteAllText((Resolve-Path $runnerPath), $runner2, [System.Text.UTF8Encoding]::new($false))
  Write-Host "Patched acquire_lock() in runner_focus_v4.py"
} else {
  throw "Could not find def acquire_lock() in runner_focus_v4.py"
}

Write-Host "`n== 4) Patch approver.py (archive sent posts instead of deleting) ==" -ForegroundColor Cyan
$approverPath = ".\approver.py"
Backup-File $approverPath
$approver = Get-Content $approverPath -Raw -Encoding UTF8

# Ensure SENT_DIR exists and replace the queue deletion with a move into ROOT/sent
# Target the specific comment + unlink block if present; otherwise do a simpler replacement.
if ($approver -notmatch 'SENT_DIR') {
  # Inject SENT_DIR right after ROOT/QUEUE definitions if possible
  $approver = $approver -replace '(?m)^(ROOT\s*=\s*Path\.home\(\)\s*/\s*"policyqueue"\s*)\r?\n(QUEUE\s*=\s*ROOT\s*/\s*"queue"\s*)',
@'
\1
\2
SENT_DIR = ROOT / "sent"
'@
}

# Ensure SENT_DIR.mkdir exists near QUEUE.mkdir
$approver = $approver -replace '(?m)^( {4}QUEUE\.mkdir\(parents=True,\s*exist_ok=True\)\s*)$',
@'
    QUEUE.mkdir(parents=True, exist_ok=True)
    SENT_DIR.mkdir(parents=True, exist_ok=True)
'@

# Replace the "remove from queue once sent" unlink with a move to sent folder
$approver = $approver -replace '(?ms)#\s*remove from queue once sent\s*\r?\n\s*p\.unlink\(missing_ok=True\)',
@'
# archive sent post (so files don't "disappear")
try:
    dst = SENT_DIR / p.name
    if dst.exists():
        # avoid clobber: add timestamp suffix
        import time
        dst = SENT_DIR / f"{p.stem}.{int(time.time())}{p.suffix}"
    p.replace(dst)
except Exception:
    # last resort: delete from queue to prevent resend loops
    p.unlink(missing_ok=True)
'@

# If the comment isn't present (different version), do a safer targeted replace of the specific unlink after save_json
# (This is a fallback and won't hit the runner's empty-file unlink.)
if ($approver -notmatch 'archive sent post') {
  $approver = $approver -replace '(?ms)(save_json\(SENT,\s*sent\)\s*\r?\n\s*)(#\s*remove from queue once sent\s*\r?\n\s*)?p\.unlink\(missing_ok=True\)',
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
Write-Host "Patched approver.py to archive sent posts into .\sent\"

Write-Host "`n== 5) Ensure folders exist ==" -ForegroundColor Cyan
$folders = @("queue","sent","logs","state","pids")
foreach ($f in $folders) { New-Item -ItemType Directory -Path (Join-Path $ROOT $f) -Force | Out-Null }

Write-Host "`n== 6) Compile-check Python files ==" -ForegroundColor Cyan
py -c "import py_compile; py_compile.compile(r'runner_focus_v4.py', doraise=True); py_compile.compile(r'approver.py', doraise=True); print('OK: runner_focus_v4.py and approver.py compile')"

Write-Host "`nDONE. Next steps:" -ForegroundColor Green
Write-Host "  1) Start runner:   py .\runner_focus_v4.py loop"
Write-Host "  2) Start approver: py .\approver.py"
Write-Host "Sent posts will now be kept in: .\sent\ (instead of being deleted)."