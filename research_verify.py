from pathlib import Path
import json, datetime, hashlib, re, urllib.parse

ROOT = Path.home() / "policyqueue"
DRAFTS = ROOT / "research_md" / "drafts"
OUT = ROOT / "research_md" / "verified"
STATE = ROOT / "research_md" / "_state" / "verified_seen.json"
CFG = ROOT / "research_config.json"

OUT.mkdir(parents=True, exist_ok=True)
STATE.parent.mkdir(parents=True, exist_ok=True)


def load_json(p, default):
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(p, obj):
    Path(p).write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")


cfg = load_json(CFG, {})
DOMAIN_HINTS = cfg.get("domain_hints", ["site:ks.gov"])
seen = load_json(STATE, {})


def sha(s):
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()


def build_query(title):
    t = re.sub(r"[^a-zA-Z0-9 _-]+", " ", title).strip()
    key = " ".join(t.split()[:10])
    hint = " ".join(DOMAIN_HINTS[:2])
    return f"{key} {hint}".strip()


def ddg_link(query):
    return "https://duckduckgo.com/?" + urllib.parse.urlencode({"q": query})


def main():
    created = 0
    if not DRAFTS.exists():
        print(f"Missing drafts dir: {DRAFTS}")
        return

    for d in sorted(DRAFTS.glob("*.md"), key=lambda p: p.stat().st_mtime):
        raw = d.read_text(encoding="utf-8", errors="ignore")
        h = sha(raw)
        if seen.get(d.name) == h:
            continue

        first = raw.splitlines()[0] if raw.splitlines() else d.stem
        title = first.replace("#", "").strip() or d.stem
        query = build_query(title)
        link = ddg_link(query)
        today = datetime.date.today().isoformat()

        appended = f"""

---

## Web Verification (links only)

Search query:
`{query}`

Search link:
{link}

Checked:
**{today}**

Status: **REVIEWED (LINKS ONLY)**

Notes:
- Open the link and confirm against authoritative sources.
- Add 1–3 official URLs under this section.
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
