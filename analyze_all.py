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
                norm_evidence.append(q + (f" Ã¢â‚¬â€ {src}" if src else ""))

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