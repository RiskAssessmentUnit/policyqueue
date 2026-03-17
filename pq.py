import os, sys, json, time, shutil, re
from pathlib import Path
from collections import Counter
from urllib.request import Request, urlopen

ROOT = Path.home() / "policyqueue"
INBOX = ROOT / "inbox"
FACTS = ROOT / "facts"
POSTS = ROOT / "posts"
ARCHIVE = ROOT / "archive"
LOGS = ROOT / "logs"
LOGFILE = LOGS / "pq.log"

OLLAMA_BASE = os.environ.get("PQ_OLLAMA_BASE", "http://100.124.222.121:11434")
MODEL = os.environ.get("PQ_MODEL", "llama3.1:8b-instruct-q4_K_M")

TIMEOUT_SEC = int(os.environ.get("PQ_TIMEOUT", "600"))
CHUNK_CHARS = int(os.environ.get("PQ_CHUNK_CHARS", "9000"))
CHUNK_OVERLAP = int(os.environ.get("PQ_CHUNK_OVERLAP", "800"))

# ✅ NEW: limit work per run (default 10)
MAX_FILES = int(os.environ.get("PQ_MAX_FILES", "10"))

def log(msg: str):
    LOGS.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    LOGFILE.open("a", encoding="utf-8").write(f"{ts}  {msg}\n")

def ensure_dirs():
    for p in (ROOT, INBOX, FACTS, POSTS, ARCHIVE, LOGS):
        p.mkdir(parents=True, exist_ok=True)

def api_check():
    req = Request(f"{OLLAMA_BASE}/api/tags", method="GET")
    with urlopen(req, timeout=10):
        pass

def pdf_to_text_pages(path: Path) -> str:
    import fitz  # PyMuPDF
    doc = fitz.open(str(path))
    chunks = []
    for i, page in enumerate(doc, start=1):
        txt = (page.get_text("text") or "").replace("\x00", "")
        chunks.append(f"\n\n[PAGE {i}]\n{txt}")
    out = "\n".join(chunks).strip()
    return out if out else "[NO_TEXT_EXTRACTED]"

def split_text(text: str, max_chars: int, overlap: int):
    text = text.strip()
    if len(text) <= max_chars:
        return [text]
    parts = []
    i = 0
    n = len(text)
    while i < n:
        j = min(i + max_chars, n)
        parts.append(text[i:j])
        if j >= n:
            break
        i = max(0, j - overlap)
    return parts

def http_post_json(url: str, payload: dict, timeout=TIMEOUT_SEC) -> dict:
    import urllib.request
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))

def ollama_generate(prompt: str) -> str:
    resp = http_post_json(
        f"{OLLAMA_BASE}/api/generate",
        {"model": MODEL, "prompt": prompt, "stream": False},
    )
    return (resp.get("response") or "").strip()

def extraction_prompt(text: str) -> str:
    return f"""You are an information extraction engine.
Output MUST be valid JSON only. No markdown. No extra text.

Task: extract Kansas public-policy / public-finance facts from the TEXT.
This may be a chunk of a longer document. Extract ONLY what is supported by this chunk.

Rules:
- If unknown, use null.
- Use numbers for numeric fields.
- Do not guess; only extract what is in the TEXT.
- Evidence quotes <= 20 words, verbatim.

Return JSON with exactly this top-level shape:
{{
  "program_type": "STAR_BOND" | "BILL" | "FISCAL_NOTE" | "AUDIT" | "NEWS" | "OTHER",
  "title": string|null,
  "jurisdiction": "Kansas"|null,
  "locations": [string],
  "entities": [
    {{ "name": string, "type": "PERSON"|"ORG"|"GOV_BODY"|"PROJECT"|"OTHER" }}
  ],
  "key_numbers": [
    {{ "label": string, "value": number, "unit": "USD"|"PERCENT"|"JOBS"|"YEAR"|"OTHER", "year": number|null }}
  ],
  "events": [
    {{ "date": string|null, "year": number|null, "description": string }}
  ],
  "evidence": [
    {{ "quote": string, "note": string }}
  ],
  "uncertainties": [string],
  "recommended_next_queries": [string]
}}

TEXT:
\"\"\"{text}\"\"\"
"""

def safe_json_parse(s: str):
    return json.loads(s)

def to_text(x):
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return str(x)

def merge_facts(parts):
    merged = {
        "program_type": "OTHER",
        "title": None,
        "jurisdiction": "Kansas",
        "locations": [],
        "entities": [],
        "key_numbers": [],
        "events": [],
        "evidence": [],
        "uncertainties": [],
        "recommended_next_queries": [],
    }

    pt = [p.get("program_type") for p in parts if p.get("program_type")]
    if pt:
        merged["program_type"] = Counter(pt).most_common(1)[0][0]

    for p in parts:
        if merged["title"] is None and p.get("title"):
            merged["title"] = p.get("title")
        if p.get("jurisdiction") == "Kansas":
            merged["jurisdiction"] = "Kansas"

    # locations
    locs, seen = [], set()
    for p in parts:
        for x in (p.get("locations") or []):
            s = to_text(x).strip()
            if not s:
                continue
            k = s.lower()
            if k in seen:
                continue
            seen.add(k)
            locs.append(s)
    merged["locations"] = locs

    # entities
    ents, seen = [], set()
    for p in parts:
        for e in (p.get("entities") or []):
            name = to_text(e.get("name")).strip()
            et = to_text(e.get("type")).strip() or "OTHER"
            if not name:
                continue
            key = (name.lower(), et.lower())
            if key in seen:
                continue
            seen.add(key)
            ents.append({"name": name, "type": et})
    merged["entities"] = ents

    # key_numbers
    kns, seen = [], set()
    for p in parts:
        for kn in (p.get("key_numbers") or []):
            label = to_text(kn.get("label")).strip()
            val = kn.get("value")
            unit = to_text(kn.get("unit")).strip() or "OTHER"
            year = kn.get("year") if isinstance(kn.get("year"), int) else None
            if not label or not isinstance(val, (int, float)):
                continue
            key = (label.lower(), float(val), unit.lower(), year or 0)
            if key in seen:
                continue
            seen.add(key)
            kns.append({"label": label, "value": val, "unit": unit, "year": year})
    merged["key_numbers"] = kns

    # events
    evs, seen = [], set()
    for p in parts:
        for ev in (p.get("events") or []):
            desc = to_text(ev.get("description")).strip()
            date = to_text(ev.get("date")).strip() if ev.get("date") else None
            year = ev.get("year") if isinstance(ev.get("year"), int) else None
            if not desc:
                continue
            key = (desc.lower(), date or "", year or 0)
            if key in seen:
                continue
            seen.add(key)
            evs.append({"date": date if date else None, "year": year, "description": desc})
    merged["events"] = evs

    # evidence
    evid, seen = [], set()
    for p in parts:
        for e in (p.get("evidence") or []):
            q = to_text(e.get("quote")).strip()
            note = to_text(e.get("note")).strip()
            if not q:
                continue
            k = q.lower()
            if k in seen:
                continue
            seen.add(k)
            evid.append({"quote": q, "note": note})
    merged["evidence"] = evid

    def merge_str_list(field):
        out, seen = [], set()
        for p in parts:
            for x in (p.get(field) or []):
                s = to_text(x).strip()
                if not s:
                    continue
                k = s.lower()
                if k in seen:
                    continue
                seen.add(k)
                out.append(s)
        return out

    merged["uncertainties"] = merge_str_list("uncertainties")
    merged["recommended_next_queries"] = merge_str_list("recommended_next_queries")
    return merged

def make_post(facts: dict) -> str:
    header_map = {
        "STAR_BOND": "Kansas STAR Bond Watch — Micro Finding",
        "FISCAL_NOTE": "Kansas Fiscal Note Watch — Micro Finding",
        "BILL": "Kansas Bill Watch — Micro Finding",
        "AUDIT": "Kansas Audit Watch — Micro Finding",
        "NEWS": "Kansas Policy Watch — Micro Finding",
        "OTHER": "Kansas Policy Watch — Micro Finding",
    }
    pt = facts.get("program_type") or "OTHER"
    header = header_map.get(pt, header_map["OTHER"])

    locs = facts.get("locations") or []
    loc = ", ".join([to_text(x).strip() for x in locs if to_text(x).strip()]) or "Kansas"
    title = to_text(facts.get("title")).strip()

    lines = [header, f"Location: {loc}"]
    if title:
        lines.append(f"Title: {title}")

    for kn in (facts.get("key_numbers") or [])[:3]:
        label = to_text(kn.get("label")).strip()
        val = kn.get("value")
        unit = to_text(kn.get("unit")).strip()
        yr = kn.get("year")
        if label and isinstance(val, (int, float)):
            tail = f" ({yr})" if isinstance(yr, int) else ""
            lines.append(f"{label}: {val} {unit}{tail}".strip())

    ev = facts.get("evidence") or []
    if ev:
        q = to_text(ev[0].get("quote")).strip()
        if q:
            lines.append(f'Evidence: "{q}"')

    lines.append("Source: internal extract (replace with primary PDF link when posting)")
    lines.append("(Tone rule: state numbers; no motive; no accusations.)")
    return "\n".join(lines)

def archive_file(src: Path):
    ts = time.strftime("%Y%m%d-%H%M%S")
    dst = ARCHIVE / f"{src.stem}.{ts}{src.suffix}"
    shutil.move(str(src), str(dst))
    return dst

def run_once():
    ensure_dirs()
    api_check()

    files = sorted(
        [p for p in INBOX.iterdir() if p.is_file() and p.suffix.lower() in (".pdf", ".txt")],
        key=lambda p: p.stat().st_mtime,
    )[:MAX_FILES]

    if not files:
        print("No files in inbox.")
        log("No files in inbox.")
        return

    log(f"Run start: will process up to {MAX_FILES} file(s). Found {len(files)}.")
    for p in files:
        log(f"Processing {p.name}")
        try:
            if p.suffix.lower() == ".txt":
                full_text = p.read_text(encoding="utf-8", errors="replace")
            else:
                full_text = pdf_to_text_pages(p)

            if not full_text.strip():
                log(f"No extracted text for {p.name} (skip)")
                continue

            chunks = split_text(full_text, CHUNK_CHARS, CHUNK_OVERLAP)
            log(f"Chunking {p.name}: total_chars={len(full_text)} chunks={len(chunks)}")

            parts = []
            for idx, ch in enumerate(chunks, start=1):
                log(f"Ollama chunk {idx}/{len(chunks)} start chars={len(ch)}")
                raw = ollama_generate(extraction_prompt(ch))
                log(f"Ollama chunk {idx}/{len(chunks)} done bytes={len(raw)}")
                try:
                    parts.append(safe_json_parse(raw))
                except Exception:
                    raw_path = FACTS / f"{p.stem}.chunk{idx}.raw.txt"
                    raw_path.write_text(raw, encoding="utf-8", errors="replace")
                    log(f"Chunk JSON parse failed; wrote {raw_path.name}")

            if not parts:
                log(f"No valid JSON chunks for {p.name}")
                continue

            merged = merge_facts(parts)

            out_json = FACTS / f"{p.stem}.json"
            out_json.write_text(json.dumps(merged, indent=2, ensure_ascii=False), encoding="utf-8")
            log(f"Wrote {out_json.name}")

            out_post = POSTS / f"{p.stem}.post.txt"
            out_post.write_text(make_post(merged), encoding="utf-8")
            log(f"Wrote {out_post.name}")

            arch = archive_file(p)
            log(f"Archived to {arch.name}")

        except Exception as e:
            log(f"ERROR {p.name}: {repr(e)}")

    print(f"Done. Facts: {FACTS}  Posts: {POSTS}")

def main():
    cmd = (sys.argv[1] if len(sys.argv) > 1 else "run").lower()
    if cmd == "run":
        run_once()
    else:
        print("Usage: python pq.py run")
        sys.exit(2)

if __name__ == "__main__":
    main()
