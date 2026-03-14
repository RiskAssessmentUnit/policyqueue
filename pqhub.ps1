# pqhub.ps1  (UNIFIED CONTROL CENTER)
# - One entrypoint for runner/approver/discord/analyzer
# - Embeds analyze_all.py and writes it out if missing (or -ForceWrite)
# - Does NOT overwrite your existing scripts unless you ask it to

param(
  [Parameter(Position=0)]
  [string]$Command = "menu",
  [switch]$ForceWrite
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Root folder (safe in PS5/PS7)
$ROOT = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $PSCommandPath }
Set-Location $ROOT

# --------- Embedded analyzer (writes markdown vault from facts/*.json) ---------
$ANALYZE_ALL_PY = @'
#!/usr/bin/env python3
import json, re, hashlib
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent
FACTS_DIR = ROOT / "facts"
OUT_DIR = ROOT / "research_md"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _safe_name(s: str) -> str:
    s = re.sub(r"[^\w\-.]+", "_", s.strip())
    return s[:180] if len(s) > 180 else s

def _read_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        return json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        return None

def _as_list(x):
    if x is None: return []
    if isinstance(x, list): return x
    return [x]

def _pick_title(doc: dict, fallback: str) -> str:
    for k in ("title","document_title","name","doc","source_title"):
        v = doc.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return fallback

def _pick_url(doc: dict) -> str:
    for k in ("url","source_url","source","link"):
        v = doc.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def _md_escape(s: str) -> str:
    return s.replace("\r","").strip()

def _hash_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()[:16]

def render_one(stem: str, doc: dict) -> str:
    title = _pick_title(doc, stem)
    url = _pick_url(doc)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    summary = doc.get("summary") or doc.get("abstract") or doc.get("tl;dr") or ""
    bullets = doc.get("bullets") or doc.get("key_points") or doc.get("highlights") or []
    entities = doc.get("entities") or {}
    numbers = doc.get("numbers") or doc.get("figures") or []
    evidence = doc.get("evidence") or doc.get("quotes") or doc.get("citations") or []
    tags = doc.get("tags") or doc.get("topics") or []

    bullets = _as_list(bullets)
    numbers = _as_list(numbers)
    evidence = _as_list(evidence)
    tags = _as_list(tags)

    norm_evidence = []
    for ev in evidence:
        if isinstance(ev, str):
            s = ev.strip()
            if s: norm_evidence.append(s)
        elif isinstance(ev, dict):
            q = ev.get("quote") or ev.get("text") or ev.get("evidence") or ""
            q = (q or "").strip()
            if q:
                src = (ev.get("source") or ev.get("url") or "").strip()
                norm_evidence.append(q + (f" â€” {src}" if src else ""))

    lines = []
    lines.append(f"# {title}")
    lines.append("")
    if url:
        lines.append(f"- Source: {url}")
    lines.append(f"- Generated: {ts}")
    lines.append(f"- Facts file: `facts/{stem}.json`")
    lines.append("")

    if summary:
        lines.append("## Summary")
        lines.append("")
        lines.append(_md_escape(str(summary)))
        lines.append("")

    if bullets:
        lines.append("## Key points")
        lines.append("")
        for b in bullets:
            if isinstance(b, str) and b.strip():
                lines.append(f"- {_md_escape(b)}")
        lines.append("")

    if entities:
        lines.append("## Entities")
        lines.append("")
        if isinstance(entities, dict):
            for k,v in entities.items():
                if isinstance(v, list):
                    vv = ", ".join([str(x) for x in v if str(x).strip()])
                else:
                    vv = str(v)
                if vv.strip():
                    lines.append(f"- **{k}**: {vv.strip()}")
        elif isinstance(entities, list):
            for e in entities:
                if str(e).strip():
                    lines.append(f"- {str(e).strip()}")
        lines.append("")

    if numbers:
        lines.append("## Numbers / figures")
        lines.append("")
        for n in numbers:
            if isinstance(n, dict):
                label = (n.get("label") or n.get("name") or "").strip()
                val = n.get("value")
                if label and val is not None:
                    lines.append(f"- **{label}**: {val}")
                else:
                    lines.append(f"- {json.dumps(n, ensure_ascii=False)}")
            else:
                s = str(n).strip()
                if s:
                    lines.append(f"- {s}")
        lines.append("")

    lines.append("## Evidence (quotes)")
    lines.append("")
    if norm_evidence:
        for q in norm_evidence[:8]:
            q = q.strip()
            if q:
                lines.append(f"> {q}")
                lines.append("")
    else:
        lines.append("> (No evidence captured in JSON)")
        lines.append("")

    if tags:
        lines.append("## Tags")
        lines.append("")
        lines.append(", ".join([str(t).strip() for t in tags if str(t).strip()]))
        lines.append("")

    raw_txt = ROOT / "facts" / f"{stem}.raw.txt"
    if raw_txt.exists():
        raw = raw_txt.read_text(encoding="utf-8", errors="ignore")
        if raw.strip():
            lines.append("## Raw excerpt")
            lines.append("")
            excerpt = raw.strip()[:4000]
            lines.append("```text")
            lines.append(excerpt)
            lines.append("```")
            lines.append("")

    body = "\n".join(lines).strip() + "\n"
    lines.append(f"<!-- content-hash:{_hash_text(body)} -->")
    return "\n".join(lines).strip() + "\n"

def write_index(rows):
    idx = OUT_DIR / "index.md"
    lines = []
    lines.append("# Research vault index")
    lines.append("")
    lines.append(f"- Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    for r in rows:
        stem, title = r["stem"], r["title"]
        lines.append(f"- [{title}](./{stem}.md)")
    idx.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

def main():
    if not FACTS_DIR.exists():
        print("facts/ folder not found. Nothing to analyze.")
        return

    rows = []
    for p in sorted(FACTS_DIR.glob("*.json")):
        stem = p.stem
        doc = _read_json(p)
        if not isinstance(doc, dict):
            continue
        title = _pick_title(doc, stem)
        md = render_one(stem, doc)
        out = OUT_DIR / f"{_safe_name(stem)}.md"
        out.write_text(md, encoding="utf-8")
        rows.append({"stem": _safe_name(stem), "title": title})

    rows.sort(key=lambda r: r["stem"], reverse=True)
    write_index(rows)
    print(f"OK: wrote {len(rows)} markdown files to {OUT_DIR}")

if __name__ == "__main__":
    main()
'@

function Write-TextNoBom([string]$Path, [string]$Content, [switch]$Force) {
  $full = Join-Path $ROOT $Path
  $exists = Test-Path $full
  if ($exists -and -not $Force) { return }
  [System.IO.File]::WriteAllText($full, $Content, [System.Text.UTF8Encoding]::new($false))
}

function Ensure-Analyzer {
  $p = Join-Path $ROOT "analyze_all.py"
  if (-not (Test-Path $p) -or $ForceWrite) {
    Write-Host "Writing analyze_all.py (no BOM)..." -ForegroundColor Cyan
    Write-TextNoBom -Path "analyze_all.py" -Content $ANALYZE_ALL_PY -Force:$true
  }
}

function Run-Py([string]$RelPath, [string[]]$Args=@()) {
  $p = Join-Path $ROOT $RelPath
  if (-not (Test-Path $p)) { throw "Missing $RelPath in $ROOT" }
  & py $p @Args
}

function Start-DetachedPy([string]$RelPath, [string[]]$Args=@(), [string]$PidFile="") {
  $p = Join-Path $ROOT $RelPath
  if (-not (Test-Path $p)) { throw "Missing $RelPath in $ROOT" }
  $argList = @("$p") + $Args
  $proc = Start-Process -FilePath "py" -ArgumentList $argList -PassThru -WindowStyle Hidden
  if ($PidFile) {
    $pidPath = Join-Path $ROOT $PidFile
    $pidDir = Split-Path -Parent $pidPath
    if (-not (Test-Path $pidDir)) { New-Item -ItemType Directory -Path $pidDir | Out-Null }
    Set-Content -Path $pidPath -Value $proc.Id -Encoding ascii
  }
  $proc.Id
}

function Stop-ByPidFile([string]$PidFile) {
  $pf = Join-Path $ROOT $PidFile
  if (-not (Test-Path $pf)) { Write-Host "No pid file: $PidFile" -ForegroundColor Yellow; return }
  $pid = (Get-Content $pf -ErrorAction SilentlyContinue | Select-Object -First 1)
  if (-not $pid) { Write-Host "Empty pid file: $PidFile" -ForegroundColor Yellow; return }
  try {
    Stop-Process -Id ([int]$pid) -Force -ErrorAction Stop
    Write-Host "Stopped PID $pid" -ForegroundColor Green
  } catch {
    Write-Host "Could not stop PID $pid (maybe already stopped)" -ForegroundColor Yellow
  }
  Remove-Item $pf -Force -ErrorAction SilentlyContinue
}

function Tail-Log([string]$RelPath, [int]$N=120) {
  $p = Join-Path $ROOT $RelPath
  if (-not (Test-Path $p)) { Write-Host "No log: $RelPath" -ForegroundColor Yellow; return }
  Get-Content $p -Tail $N
}

function Menu {
  while ($true) {
    Write-Host ""
    Write-Host "=== POLICYQUEUE HUB (pqhub.ps1) ===" -ForegroundColor Cyan
    Write-Host "Root: $ROOT" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "1) Start runner (runner_focus_v4.py loop)"
    Write-Host "2) Stop runner (pids\runner_focus_v4.pid)"
    Write-Host "3) Start approver (approver.py loop)"
    Write-Host "4) Stop approver (pids\approver.pid)"
    Write-Host "5) Start discord notifier (discord_notifier.py loop)"
    Write-Host "6) Stop discord notifier (pids\discord.pid)"
    Write-Host "7) Analyze vault now (writes research_md/*.md)"
    Write-Host "8) Tail runner log"
    Write-Host "9) Tail approver log"
    Write-Host "10) Tail pq log"
    Write-Host "11) Open folders (queue, posts, research_md)"
    Write-Host "12) Exit"
    $c = Read-Host "Choose"
    switch ($c) {
      "1" {
        $id = Start-DetachedPy "runner_focus_v4.py" @("loop") "pids\runner_focus_v4.pid"
        Write-Host "Runner started PID=$id" -ForegroundColor Green
      }
      "2" { Stop-ByPidFile "pids\runner_focus_v4.pid" }
      "3" {
        $id = Start-DetachedPy "approver.py" @("loop") "pids\approver.pid"
        Write-Host "Approver started PID=$id" -ForegroundColor Green
      }
      "4" { Stop-ByPidFile "pids\approver.pid" }
      "5" {
        $id = Start-DetachedPy "discord_notifier.py" @("loop") "pids\discord.pid"
        Write-Host "Discord notifier started PID=$id" -ForegroundColor Green
      }
      "6" { Stop-ByPidFile "pids\discord.pid" }
      "7" { Ensure-Analyzer; Run-Py "analyze_all.py" }
      "8" { Tail-Log "logs\runner.log" 160 }
      "9" { Tail-Log "logs\approver.log" 160 }
      "10" { Tail-Log "logs\pq.log" 160 }
      "11" {
        Start-Process (Join-Path $ROOT "queue")
        Start-Process (Join-Path $ROOT "posts")
        Start-Process (Join-Path $ROOT "research_md")
      }
      "12" { return }
      default { Write-Host "?" -ForegroundColor Yellow }
    }
  }
}

switch ($Command.ToLowerInvariant()) {
  "menu" { Menu }
  "analyze" { Ensure-Analyzer; Run-Py "analyze_all.py" }
  "runner-start" { Start-DetachedPy "runner_focus_v4.py" @("loop") "pids\runner_focus_v4.pid" | Out-Null; "OK" }
  "runner-stop" { Stop-ByPidFile "pids\runner_focus_v4.pid" }
  "approver-start" { Start-DetachedPy "approver.py" @("loop") "pids\approver.pid" | Out-Null; "OK" }
  "approver-stop" { Stop-ByPidFile "pids\approver.pid" }
  "discord-start" { Start-DetachedPy "discord_notifier.py" @("loop") "pids\discord.pid" | Out-Null; "OK" }
  "discord-stop" { Stop-ByPidFile "pids\discord.pid" }
  default { Menu }
}