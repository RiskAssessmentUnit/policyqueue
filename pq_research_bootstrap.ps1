# pq_research_bootstrap.ps1
# Safe: READS facts/, WRITES research_md/. Does NOT touch runner/approver/queue/sent.

$ErrorActionPreference = "Stop"

$ROOT = (Get-Location).Path
if (-not (Test-Path (Join-Path $ROOT "facts"))) {
  throw "Run this from your policyqueue folder (where facts/ exists). Current: $ROOT"
}

Write-Host "== PQ Research Bootstrap (SAFE) =="

# --- folders ---
$dirs = @(
  "research_md",
  "research_md\drafts",
  "research_md\verified",
  "research_md\logs",
  "research_md\_state"
)
foreach ($d in $dirs) { New-Item -ItemType Directory -Force -Path (Join-Path $ROOT $d) | Out-Null }

# --- config ---
$configPath = Join-Path $ROOT "research_config.json"
if (-not (Test-Path $configPath)) {
@'
{
  "keywords": [
    "star bond",
    "tax revenue",
    "sales tax",
    "income tax",
    "consensus revenue",
    "appropriation",
    "budget",
    "fiscal note",
    "committee",
    "minutes"
  ],
  "max_json_chars_in_draft": 9000,
  "only_new_drafts": true,
  "domain_hints": ["site:ks.gov", "site:kansas.gov", "site:kslegislature.org"],
  "verify_mode": "links_only"
}
'@ | Set-Content -Encoding UTF8 $configPath
  Write-Host "Wrote research_config.json"
} else {
  Write-Host "Kept existing research_config.json"
}

# --- research_drafts.py ---
$draftsPy = Join-Path $ROOT "research_drafts.py"
@'
from __future__ import annotations
from pathlib import Path
import json, datetime, hashlib

ROOT = Path.home() / "policyqueue"
FACTS = ROOT / "facts"
OUT  = ROOT / "research_md" / "drafts"
STATE = ROOT / "research_md" / "_state" / "drafts_seen.json"
CFG  = ROOT / "research_config.json"

OUT.mkdir(parents=True, exist_ok=True)
STATE.parent.mkdir(parents=True, exist_ok=True)

def load_json(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def save_json(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")

cfg = load_json(CFG, {})
KEYWORDS = [k.lower() for k in cfg.get("keywords", [])]
MAX_CHARS = int(cfg.get("max_json_chars_in_draft", 9000))
ONLY_NEW = bool(cfg.get("only_new_drafts", True))

seen = load_json(STATE, {}) if ONLY_NEW else {}

def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()

def matches(text: str) -> list[str]:
    t = text.lower()
    hits = []
    for kw in KEYWORDS:
        if kw and kw in t:
            hits.append(kw)
    return hits

def summarize_local(data) -> str:
    # Keep it dead simple: we include a trimmed JSON excerpt + keys overview.
    try:
        keys = list(data.keys()) if isinstance(data, dict) else []
    except Exception:
        keys = []
    excerpt = json.dumps(data, indent=2)[:MAX_CHARS]
    return f"Top-level keys: {keys}\n\n```json\n{excerpt}\n```"

def main():
    if not FACTS.exists():
        print(f"Missing facts dir: {FACTS}")
        return

    created = 0
    for f in sorted(FACTS.glob("*.json"), key=lambda p: p.stat().st_mtime):
        raw = f.read_text(encoding="utf-8", errors="ignore")
        h = sha(raw)
        if ONLY_NEW and seen.get(f.name) == h:
            continue

        try:
            data = json.loads(raw)
        except Exception:
            continue

        hits = matches(raw)
        if not hits:
            if ONLY_NEW:
                seen[f.name] = h
            continue

        out_name = f"{f.stem}.md"
        out_path = OUT / out_name
        if out_path.exists() and ONLY_NEW:
            seen[f.name] = h
            continue

        today = datetime.date.today().isoformat()
        local = summarize_local(data)
        hitline = ", ".join(hits[:6])

        md = f"""# Research Draft: {f.stem}

- Source JSON: `{f.name}`
- Keyword hits: **{hitline}**
- Generated: **{today}**
- Status: **UNVERIFIED**

## Local Findings (from facts/)
{local}

## Verification Checklist
- Find authoritative page(s) confirming the topic
- Confirm any key numbers/dates against sources
- Note what changed (if anything) since the PDF/source date

## Web Verification Notes
_TODO_
"""

        out_path.write_text(md, encoding="utf-8")
        print("DRAFT:", out_path.name)
        created += 1

        if ONLY_NEW:
            seen[f.name] = h

    if ONLY_NEW:
        save_json(STATE, seen)

    print(f"Done. Drafts created: {created}")

if __name__ == "__main__":
    main()
'@ | Set-Content -Encoding UTF8 $draftsPy
Write-Host "Wrote research_drafts.py"

# --- research_verify.py ---
$verifyPy = Join-Path $ROOT "research_verify.py"
@'
from __future__ import annotations
from pathlib import Path
import json, datetime, re, hashlib
import urllib.parse

ROOT = Path.home() / "policyqueue"
DRAFTS = ROOT / "research_md" / "drafts"
OUT    = ROOT / "research_md" / "verified"
STATE  = ROOT / "research_md" / "_state" / "verified_seen.json"
CFG    = ROOT / "research_config.json"

OUT.mkdir(parents=True, exist_ok=True)
STATE.parent.mkdir(parents=True, exist_ok=True)

def load_json(p: Path, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def save_json(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")

cfg = load_json(CFG, {})
DOMAIN_HINTS = cfg.get("domain_hints", ["site:ks.gov"])
VERIFY_MODE = cfg.get("verify_mode", "links_only")  # "links_only" keeps it safe/offline.

seen = load_json(STATE, {})

def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()

def build_query(title: str, body: str) -> str:
    # Extract a few meaningful tokens from the title/body
    t = re.sub(r"[^a-zA-Z0-9 _-]+", " ", title).strip()
    key = " ".join(t.split()[:10])
    hint = " ".join(DOMAIN_HINTS[:2])
    q = f"{key} {hint}".strip()
    return q

def duckduckgo_link(query: str) -> str:
    return "https://duckduckgo.com/?" + urllib.parse.urlencode({"q": query})

def main():
    created = 0
    for d in sorted(DRAFTS.glob("*.md"), key=lambda p: p.stat().st_mtime):
        raw = d.read_text(encoding="utf-8", errors="ignore")
        h = sha(raw)
        if seen.get(d.name) == h:
            continue

        # Title line
        first = raw.splitlines()[0] if raw.splitlines() else d.stem
        title = first.replace("#", "").strip() or d.stem

        q = build_query(title, raw)
        link = duckduckgo_link(q)
        today = datetime.date.today().isoformat()

        if VERIFY_MODE == "links_only":
            appended = f"""
---

## Web Verification (links only)

Search query:
`{q}`

Search link:
{link}

Checked:
**{today}**

Status: **REVIEWED (LINKS ONLY)**

Notes:
- Open the link and paste 1–3 authoritative URLs here when ready.
"""
        else:
            appended = f"""
---

## Web Verification

Mode `{VERIFY_MODE}` not implemented in this offline-safe script.

Search query:
`{q}`

Search link:
{link}

Checked:
**{today}**
"""

        out_path = OUT / d.name
        out_path.write_text(raw + appended, encoding="utf-8")
        print("VERIFIED:", out_path.name)
        created += 1
        seen[d.name] = h

    save_json(STATE, seen)
    print(f"Done. Verified files written: {created}")

if __name__ == "__main__":
    main()
'@ | Set-Content -Encoding UTF8 $verifyPy
Write-Host "Wrote research_verify.py"

# --- researchctl.ps1 (launcher) ---
$ctl = Join-Path $ROOT "researchctl.ps1"
@'
param(
  [ValidateSet("draft","verify","all")]
  [string]$cmd = "all"
)

$ErrorActionPreference = "Stop"
$root = (Get-Location).Path

function RunPy($file) {
  if (-not (Test-Path (Join-Path $root $file))) { throw "Missing $file" }
  py (Join-Path $root $file)
}

switch ($cmd) {
  "draft"  { RunPy "research_drafts.py" }
  "verify" { RunPy "research_verify.py" }
  "all"    { RunPy "research_drafts.py"; RunPy "research_verify.py" }
}
'@ | Set-Content -Encoding UTF8 $ctl
Write-Host "Wrote researchctl.ps1"

Write-Host ""
Write-Host "== DONE =="
Write-Host "Run:  .\researchctl.ps1 all"
Write-Host "Drafts:   .\research_md\drafts"
Write-Host "Verified: .\research_md\verified"
Write-Host ""
Write-Host "Optional: create a scheduled task that runs every morning:"
Write-Host '  .\researchctl.ps1 all'
Write-Host ""
Write-Host "NOTE: verify step is LINKS ONLY (safe). It generates search links; it does not hit the web itself."
'@ | Set-Content -Encoding UTF8 (Join-Path $ROOT "pq_research_bootstrap.ps1") -NoNewline

Write-Host "Bootstrap script written to pq_research_bootstrap.ps1"
Write-Host "Now run:  powershell -ExecutionPolicy Bypass -File .\pq_research_bootstrap.ps1"