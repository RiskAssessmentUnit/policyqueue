from __future__ import annotations

import os
import re
import sqlite3
import time
import hashlib
from pathlib import Path
from typing import Optional, Dict, Tuple

ROOT = Path(os.environ.get("PQ_ROOT", str(Path.home() / "policyqueue"))).resolve()
DB_PATH = Path(os.environ.get("PQ_DB", str(ROOT / "pq.sqlite"))).resolve()

POSTS_DIR = ROOT / "posts"

# PDF buckets we consider "valid locations" for the source PDF
PDF_DIRS = [
    ROOT / "inbox",
    ROOT / "archive",
    ROOT / "bigpdfs",
    ROOT / "junk",
    ROOT,  # last-resort: any PDFs sitting in root
]

# If your filenames are like "foo_abcdef12.pdf" (hash suffix), we try stripping the suffix.
HASH_SUFFIX_RE = re.compile(r"^(?P<base>.+?)_[0-9a-f]{6,32}$", re.IGNORECASE)

def sha256_file(p: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(DB_PATH))
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con

def ensure_tables(con: sqlite3.Connection) -> None:
    # idempotent; safe even if already exists
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS pdf_posts (
            sha256 TEXT PRIMARY KEY,
            pdf_relpath TEXT NOT NULL,
            post_relpath TEXT NOT NULL,
            score INTEGER,
            posted_ts INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pdf_posts_posted_ts ON pdf_posts(posted_ts);
        """
    )
    con.commit()

def normalize_stem(stem: str) -> str:
    # strip common suffix hash pattern
    m = HASH_SUFFIX_RE.match(stem)
    if m:
        return m.group("base")
    return stem

def build_pdf_index() -> Dict[str, Path]:
    """
    Build a map from possible stems -> pdf path.
    We map:
      - exact stem
      - normalized stem with hash suffix stripped
    For collisions, we keep the newest file.
    """
    idx: Dict[str, Path] = {}
    for d in PDF_DIRS:
        if not d.exists():
            continue
        for p in d.glob("*.pdf"):
            try:
                key1 = p.stem
                key2 = normalize_stem(p.stem)
                for k in {key1, key2}:
                    if k not in idx:
                        idx[k] = p
                    else:
                        # keep the newest
                        if p.stat().st_mtime > idx[k].stat().st_mtime:
                            idx[k] = p
            except Exception:
                continue

    # final fallback: recurse under ROOT (slow but catches weird placements)
    # only if we still have very little indexed
    if len(idx) < 10:
        for p in ROOT.rglob("*.pdf"):
            try:
                key1 = p.stem
                key2 = normalize_stem(p.stem)
                for k in {key1, key2}:
                    if k not in idx:
                        idx[k] = p
                    else:
                        if p.stat().st_mtime > idx[k].stat().st_mtime:
                            idx[k] = p
            except Exception:
                continue

    return idx

def parse_score_from_post(text: str) -> Optional[int]:
    # If you later add "score=NN" into the post file, we’ll capture it.
    m = re.search(r"\bscore\s*=\s*(\d+)\b", text, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None

def upsert_pdf_posted(
    con: sqlite3.Connection,
    sha256: str,
    pdf_path: Path,
    post_path: Path,
    score: Optional[int],
    ts_epoch: int
) -> None:
    pdf_rel = str(pdf_path.resolve().relative_to(ROOT)).replace("\\", "/")
    post_rel = str(post_path.resolve().relative_to(ROOT)).replace("\\", "/")
    con.execute(
        """
        INSERT INTO pdf_posts(sha256, pdf_relpath, post_relpath, score, posted_ts)
        VALUES(?,?,?,?,?)
        ON CONFLICT(sha256) DO UPDATE SET
            pdf_relpath=excluded.pdf_relpath,
            post_relpath=excluded.post_relpath,
            score=COALESCE(excluded.score, pdf_posts.score),
            posted_ts=MAX(pdf_posts.posted_ts, excluded.posted_ts)
        """,
        (sha256, pdf_rel, post_rel, score, ts_epoch),
    )

def main() -> None:
    t0 = time.time()

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    con = connect()
    ensure_tables(con)

    pdf_index = build_pdf_index()

    posts = sorted(POSTS_DIR.glob("*.post.txt"), key=lambda p: p.stat().st_mtime)
    posts_found = len(posts)

    synced = 0
    missing = 0
    hashed = 0

    for post_path in posts:
        stem = post_path.stem  # "foo.post"?? actually Path.stem strips one suffix; for foo.post.txt => foo.post
        # We want foo (without .post) if your naming is foo.post.txt
        if stem.endswith(".post"):
            stem = stem[:-5]

        # lookup candidates
        cand_keys = [stem, normalize_stem(stem)]
        pdf_path = None
        for k in cand_keys:
            pdf_path = pdf_index.get(k)
            if pdf_path and pdf_path.exists():
                break

        if not pdf_path:
            missing += 1
            continue

        # hash + insert
        try:
            sha = sha256_file(pdf_path)
            hashed += 1
            txt = post_path.read_text(encoding="utf-8", errors="replace")
            score = parse_score_from_post(txt)
            ts_epoch = int(post_path.stat().st_mtime)
            upsert_pdf_posted(con, sha, pdf_path, post_path, score, ts_epoch)
            synced += 1
        except Exception:
            # don’t hard-fail; treat as missing-ish
            missing += 1
            continue

    con.commit()
    con.close()

    dt = time.time() - t0
    print(f"OK: synced posts into DB. posts_found={posts_found} synced={synced} missing_pdf={missing} pdf_hashed={hashed} seconds={dt:.2f}")

if __name__ == "__main__":
    main()
