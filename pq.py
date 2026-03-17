import os, sys, json, time, shutil, re
from pathlib import Path
from collections import Counter

import extract as extract_mod

ROOT = Path.home() / "policyqueue"
INBOX = ROOT / "inbox"
FACTS = ROOT / "facts"
POSTS = ROOT / "posts"
ARCHIVE = ROOT / "archive"
LOGS = ROOT / "logs"
LOGFILE = LOGS / "pq.log"

# Chunking kept for very large docs; individual chunks still go through Claude
CHUNK_CHARS   = int(os.environ.get("PQ_CHUNK_CHARS", "9000"))
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

# Delegate all LLM work and fact merging to extract.py
merge_facts   = extract_mod.merge_facts

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

            log(f"Extracting facts from {p.name}: total_chars={len(full_text)}")
            merged = extract_mod.extract_facts(full_text)

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
