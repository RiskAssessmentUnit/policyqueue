"""
processor.py — PDF text extraction, fact extraction, scoring, and queueing.

Public entry points:
  process_one(pdf_path, source_url)  — process a single PDF
  pick_files(folder, limit)          — list oldest PDFs up to limit
"""

import json
import os
import shutil
import time
from pathlib import Path

import db
import extract as extract_mod

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT    = Path(__file__).resolve().parent
FACTS   = ROOT / "facts"
QUEUE   = ROOT / "queue"
ARCHIVE = ROOT / "archive"
BIGBOX  = ROOT / "big"
LOGS    = ROOT / "logs"
RUNLOG  = LOGS / "runner.log"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

POST_SCORE_MIN   = int(os.environ.get("PQ_POST_SCORE_MIN", "6"))
REQUIRE_EVIDENCE = os.environ.get("PQ_REQUIRE_EVIDENCE", "1").strip() != "0"

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    LOGS.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    RUNLOG.open("a", encoding="utf-8").write(f"{ts}  {msg}\n")


def _pdf_to_text(path: Path) -> str:
    import fitz
    doc = fitz.open(str(path))
    chunks = []
    for i, page in enumerate(doc, start=1):
        txt = (page.get_text("text") or "").replace("\x00", "")
        chunks.append(f"\n\n[PAGE {i}]\n{txt}")
    out = "\n".join(chunks).strip()
    return out if out else "[NO_TEXT_EXTRACTED]"


def _archive_file(src: Path) -> Path:
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    ts  = time.strftime("%Y%m%d-%H%M%S")
    dst = ARCHIVE / f"{src.stem}.{ts}{src.suffix}"
    shutil.move(str(src), str(dst))
    return dst


def _queue_post(post_text: str, pdf_name: str) -> str:
    """Write post to queue/ if not a duplicate. Returns filename or ''."""
    h = db.sha256_text(post_text)
    if db.is_post_hash_seen(h):
        return ""
    QUEUE.mkdir(parents=True, exist_ok=True)
    out = QUEUE / f"{Path(pdf_name).stem}.post.txt"
    if out.exists():
        out = QUEUE / f"{Path(pdf_name).stem}_{h[:8]}.post.txt"
    out.write_text(post_text, encoding="utf-8")
    db.save_post_hash(h, out.name)
    return out.name


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def pick_files(folder: Path, limit: int) -> list:
    """Return up to `limit` PDFs from `folder`, oldest first."""
    files = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == ".pdf"]
    files.sort(key=lambda p: p.stat().st_mtime)
    return files[:limit]


def process_one(pdf_path: Path, source_url: str) -> None:
    """Extract facts from a PDF, score it, and queue a post if it passes."""
    try:
        import fitz  # noqa: ensure installed
    except Exception:
        _log(f"PROCESS ERROR {pdf_path.name}: PyMuPDF missing")
        _archive_file(pdf_path)
        return

    text  = _pdf_to_text(pdf_path)
    facts = extract_mod.extract_facts(text, source_url)
    score = extract_mod.score_facts(facts)

    FACTS.mkdir(parents=True, exist_ok=True)
    (FACTS / f"{pdf_path.stem}.json").write_text(
        json.dumps(facts, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    if REQUIRE_EVIDENCE and not facts.get("evidence"):
        _log(f"SKIP {pdf_path.name} no evidence")
        db.update_pdf_processed(pdf_path.name, score, None)
        db.add_event("PROCESS_SKIP", pdf_name=pdf_path.name, details="no evidence")
        _archive_file(pdf_path)
        return

    if score < POST_SCORE_MIN:
        _log(f"SKIP {pdf_path.name} score={score} below threshold")
        db.update_pdf_processed(pdf_path.name, score, None)
        db.add_event("PROCESS_SKIP", pdf_name=pdf_path.name, details=f"score={score}")
        _archive_file(pdf_path)
        return

    post = extract_mod.generate_post(facts, source_url, pdf_path.name)
    if not post.strip():
        _log(f"SKIP {pdf_path.name} writer empty")
        db.update_pdf_processed(pdf_path.name, score, None)
        db.add_event("PROCESS_SKIP", pdf_name=pdf_path.name, details="writer empty")
        _archive_file(pdf_path)
        return

    queued = _queue_post(post, pdf_path.name)
    if queued:
        _log(f"QUEUED {queued}")
        db.update_pdf_processed(pdf_path.name, score, queued)
        db.add_event("PROCESS_POSTED", pdf_name=pdf_path.name, details=f"score={score} post={queued}")
    _archive_file(pdf_path)
