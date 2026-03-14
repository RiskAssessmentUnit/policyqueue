from pathlib import Path
import json, datetime, hashlib

ROOT = Path.home() / "policyqueue"
FACTS = ROOT / "facts"
OUT = ROOT / "research_md" / "drafts"
STATE = ROOT / "research_md" / "_state" / "drafts_seen.json"
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
KEYWORDS = [k.lower() for k in cfg.get("keywords", [])]
MAX_CHARS = int(cfg.get("max_json_chars_in_draft", 9000))
ONLY_NEW = bool(cfg.get("only_new_drafts", True))
seen = load_json(STATE, {}) if ONLY_NEW else {}


def sha(s):
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()


def matches(text):
    t = text.lower()
    return [kw for kw in KEYWORDS if kw and kw in t]


def main():
    created = 0
    if not FACTS.exists():
        print(f"Missing facts dir: {FACTS}")
        return

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

        out_path = OUT / f"{f.stem}.md"
        if out_path.exists() and ONLY_NEW:
            seen[f.name] = h
            continue

        excerpt = json.dumps(data, indent=2)[:MAX_CHARS]
        today = datetime.date.today().isoformat()

        md = f"""# Research Draft: {f.stem}

- Source JSON: `{f.name}`
- Keyword hits: **{", ".join(hits[:6])}**
- Generated: **{today}**
- Status: **UNVERIFIED**

## Local Findings
```json
{excerpt}
```

## Verification Checklist
- Confirm with authoritative government pages
- Confirm numbers/dates if present
- Note whether the local finding is current or outdated

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
