# pq_runner_lock_fix.ps1
# Repairs runner_focus_v4.py after a bad patch by:
# 1) Removing any previously injected "if os.name == 'nt':" block near the lock check (best-effort)
# 2) Adding a top-level helper pid_is_running(pid)
# 3) Replacing os.kill(int(old), 0) with pid_is_running()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$path = ".\runner_focus_v4.py"
if (!(Test-Path $path)) { throw "runner_focus_v4.py not found. Run from C:\Users\meta4\policyqueue" }

function Backup-File($p) {
  $ts = Get-Date -Format "yyyyMMdd-HHmmss"
  Copy-Item $p "$p.bak.$ts" -Force
}

Backup-File $path
$txt = Get-Content $path -Raw -Encoding UTF8

# 1) Best-effort remove the previously injected broken block (the one that starts with: if os.name == "nt":)
# We only remove the specific injected snippet shape if it exists.
$txt = [regex]::Replace($txt, '(?ms)^\s*if\s+os\.name\s*==\s*["'']nt["'']:\s*\R\s*import\s+subprocess\s*\R\s*r\s*=\s*subprocess\.run\(\[["'']tasklist["''],\s*["'']/FI["''],\s*f["'']PID eq \{int\(old\)\}["'']\],\s*capture_output=True,\s*text=True\)\s*\R\s*if\s+str\(int\(old\)\)\s+in\s+\(r\.stdout\s+or\s+["'']["'']\):\s*\R\s*raise\s+SystemExit\([^\)]*\)\s*\R\s*else:\s*\R\s*os\.kill\([^\)]*\)\s*$', '', 'Multiline')

# 2) Ensure helper exists near the top (after imports). Add it if missing.
if ($txt -notmatch '(?m)^def\s+pid_is_running\('