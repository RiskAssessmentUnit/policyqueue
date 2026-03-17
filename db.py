"""
db.py — Single SQLite state layer for PolicyQueue.

Replaces four JSON state files:
  state/seen_urls.json      → urls table
  state/seen_pdfs.json      → pdfs table (source_url + filename columns)
  state/seen_hashes.json    → pdfs table (sha256 column)
  state/seen_posthash.json  → post_hashes table

Also the authoritative write path for events, replacing pq_db_ingest.py
(which rebuilt state from log-file regex — now state is written directly here).

Usage:
    import db
    db.init(ROOT / "pq.sqlite")   # call once at startup
    db.is_url_seen(url)           # check before crawling
    db.mark_url_seen(url)         # after fetching
    ...
"""

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS urls (
  url            TEXT PRIMARY KEY,
  domain         TEXT,
  first_seen_ts  INTEGER,
  last_seen_ts   INTEGER,
  status         TEXT,          -- ok | error | skip
  last_http_code INTEGER,
  last_error     TEXT
);

CREATE TABLE IF NOT EXISTS pdfs (
  pdf_id            INTEGER PRIMARY KEY AUTOINCREMENT,
  sha256            TEXT UNIQUE,
  bytes             INTEGER,
  filename          TEXT,
  source_url        TEXT,
  domain            TEXT,
  saved_path        TEXT,
  first_seen_ts     INTEGER,
  last_seen_ts      INTEGER,
  stage             TEXT,       -- inbox | big | posted | archive | junk | unknown
  signal_score      INTEGER,
  last_processed_ts INTEGER,
  last_post_path    TEXT,
  FOREIGN KEY(source_url) REFERENCES urls(url) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS idx_pdfs_stage     ON pdfs(stage);
CREATE INDEX IF NOT EXISTS idx_pdfs_last_seen ON pdfs(last_seen_ts);
CREATE INDEX IF NOT EXISTS idx_pdfs_domain    ON pdfs(domain);
CREATE INDEX IF NOT EXISTS idx_pdfs_filename  ON pdfs(filename);

CREATE TABLE IF NOT EXISTS post_hashes (
  sha256     TEXT PRIMARY KEY,
  filename   TEXT NOT NULL,
  created_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts       INTEGER,
  kind     TEXT,     -- SAVED|DUP|ERROR|PROCESS_POSTED|PROCESS_SKIP|CYCLE_DONE
  domain   TEXT,
  url      TEXT,
  pdf_name TEXT,
  details  TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_ts   ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind);
"""

# ---------------------------------------------------------------------------
# Module-level singleton connection (runner is single-threaded)
# ---------------------------------------------------------------------------

_con: Optional[sqlite3.Connection] = None
_db_path: Optional[Path] = None


def init(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Open the database, apply schema, migrate any existing JSON state files."""
    global _con, _db_path

    if db_path is not None:
        _db_path = Path(db_path)
    if _db_path is None:
        _db_path = Path(os.environ.get(
            "PQ_DB",
            str(Path.home() / "policyqueue" / "pq.sqlite")
        ))

    _db_path.parent.mkdir(parents=True, exist_ok=True)
    _con = sqlite3.connect(str(_db_path), check_same_thread=False)
    _con.executescript(_SCHEMA)
    _con.execute("INSERT OR IGNORE INTO meta(k,v) VALUES(?,?)", ("schema_version", "2"))
    _con.commit()

    _migrate_json_state(_db_path.parent / "state")
    return _con


def _get() -> sqlite3.Connection:
    if _con is None:
        raise RuntimeError("db.init() has not been called")
    return _con


def reset() -> None:
    """Close the connection and clear module state. For use in tests only."""
    global _con, _db_path
    if _con is not None:
        try:
            _con.close()
        except Exception:
            pass
        _con = None
    _db_path = None


# ---------------------------------------------------------------------------
# One-time migration from JSON state files
# ---------------------------------------------------------------------------

def _migrate_json_state(state_dir: Path) -> None:
    """Import legacy JSON state files into SQLite, then rename them to .bak."""
    migrations = {
        "seen_urls.json":     _migrate_seen_urls,
        "seen_pdfs.json":     _migrate_seen_pdfs,
        "seen_hashes.json":   _migrate_seen_hashes,
        "seen_posthash.json": _migrate_seen_posthash,
    }
    for filename, fn in migrations.items():
        src = state_dir / filename
        if not src.exists():
            continue
        try:
            data = json.loads(src.read_text(encoding="utf-8", errors="replace"))
            fn(data)
            src.rename(src.with_suffix(".json.bak"))
        except Exception:
            pass  # leave the file in place if migration fails
    _get().commit()


def _migrate_seen_urls(data) -> None:
    if not isinstance(data, list):
        return
    now = int(time.time())
    for url in data:
        if not isinstance(url, str):
            continue
        domain = _domain(url)
        _get().execute("""
            INSERT OR IGNORE INTO urls(url, domain, first_seen_ts, last_seen_ts, status)
            VALUES(?,?,?,?,'ok')
        """, (url, domain, now, now))


def _migrate_seen_pdfs(data) -> None:
    """seen_pdfs = {url: filename}"""
    if not isinstance(data, dict):
        return
    now = int(time.time())
    for url, filename in data.items():
        if not (isinstance(url, str) and isinstance(filename, str)):
            continue
        domain = _domain(url)
        _get().execute("""
            INSERT OR IGNORE INTO urls(url, domain, first_seen_ts, last_seen_ts, status)
            VALUES(?,?,?,?,'ok')
        """, (url, domain, now, now))
        _get().execute("""
            INSERT INTO pdfs(filename, source_url, domain, first_seen_ts, last_seen_ts, stage)
            VALUES(?,?,?,?,?,'archive')
            ON CONFLICT(sha256) DO NOTHING
        """, (filename, url, domain, now, now))


def _migrate_seen_hashes(data) -> None:
    """seen_hashes = {sha256: filename}"""
    if not isinstance(data, dict):
        return
    now = int(time.time())
    for sha, filename in data.items():
        if not (isinstance(sha, str) and isinstance(filename, str)):
            continue
        _get().execute("""
            INSERT INTO pdfs(sha256, filename, first_seen_ts, last_seen_ts, stage)
            VALUES(?,?,?,?,'archive')
            ON CONFLICT(sha256) DO UPDATE SET
                filename = COALESCE(excluded.filename, pdfs.filename)
        """, (sha, filename, now, now))


def _migrate_seen_posthash(data) -> None:
    """seen_posthash = {sha256: filename}"""
    if not isinstance(data, dict):
        return
    now = int(time.time())
    for sha, filename in data.items():
        if not (isinstance(sha, str) and isinstance(filename, str)):
            continue
        _get().execute("""
            INSERT OR IGNORE INTO post_hashes(sha256, filename, created_ts)
            VALUES(?,?,?)
        """, (sha, filename, now))


# ---------------------------------------------------------------------------
# URL state  (replaces seen_urls.json)
# ---------------------------------------------------------------------------

def is_url_seen(url: str) -> bool:
    row = _get().execute("SELECT 1 FROM urls WHERE url=?", (url,)).fetchone()
    return row is not None


def mark_url_seen(
    url: str,
    status: str = "ok",
    http_code: Optional[int] = None,
    error: Optional[str] = None,
) -> None:
    domain = _domain(url)
    now = int(time.time())
    _get().execute("""
        INSERT INTO urls(url, domain, first_seen_ts, last_seen_ts, status, last_http_code, last_error)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(url) DO UPDATE SET
            last_seen_ts   = excluded.last_seen_ts,
            status         = excluded.status,
            last_http_code = COALESCE(excluded.last_http_code, urls.last_http_code),
            last_error     = COALESCE(excluded.last_error, urls.last_error)
    """, (url, domain, now, now, status, http_code, error))
    _get().commit()


# ---------------------------------------------------------------------------
# PDF hash / URL state  (replaces seen_hashes.json + seen_pdfs.json)
# ---------------------------------------------------------------------------

def get_filename_for_hash(sha256: str) -> Optional[str]:
    """Return existing filename if this content hash is already saved."""
    row = _get().execute("SELECT filename FROM pdfs WHERE sha256=?", (sha256,)).fetchone()
    return row[0] if row else None


def is_pdf_url_seen(url: str) -> bool:
    """True if this URL has already been downloaded as a PDF."""
    row = _get().execute("SELECT 1 FROM pdfs WHERE source_url=?", (url,)).fetchone()
    return row is not None


def get_url_for_filename(filename: str) -> str:
    """Reverse-lookup: filename → source URL (for process_one)."""
    row = _get().execute("SELECT source_url FROM pdfs WHERE filename=?", (filename,)).fetchone()
    return (row[0] or "UNKNOWN_URL") if row else "UNKNOWN_URL"


def save_pdf(
    sha256: str,
    filename: str,
    source_url: str,
    size_bytes: int,
    saved_path: str,
) -> None:
    """Record a newly downloaded PDF and mark its URL seen."""
    domain = _domain(source_url)
    now = int(time.time())
    mark_url_seen(source_url)
    _get().execute("""
        INSERT INTO pdfs(sha256, bytes, filename, source_url, domain,
                         saved_path, first_seen_ts, last_seen_ts, stage)
        VALUES(?,?,?,?,?,?,?,?,'inbox')
        ON CONFLICT(sha256) DO UPDATE SET
            last_seen_ts = excluded.last_seen_ts,
            source_url   = COALESCE(excluded.source_url, pdfs.source_url),
            filename     = COALESCE(excluded.filename, pdfs.filename)
    """, (sha256, size_bytes, filename, source_url, domain, saved_path, now, now))
    _get().commit()


def update_pdf_processed(
    filename: str,
    score: int,
    post_path: Optional[str],
) -> None:
    """Update stage and score after processing."""
    stage = "posted" if post_path else "processed"
    now = int(time.time())
    _get().execute("""
        UPDATE pdfs
        SET stage=?, signal_score=?, last_processed_ts=?, last_post_path=?
        WHERE filename=?
    """, (stage, score, now, post_path, filename))
    _get().commit()


# ---------------------------------------------------------------------------
# Post-hash state  (replaces seen_posthash.json)
# ---------------------------------------------------------------------------

def is_post_hash_seen(sha256: str) -> bool:
    row = _get().execute("SELECT 1 FROM post_hashes WHERE sha256=?", (sha256,)).fetchone()
    return row is not None


def save_post_hash(sha256: str, filename: str) -> None:
    _get().execute(
        "INSERT OR IGNORE INTO post_hashes(sha256, filename, created_ts) VALUES(?,?,?)",
        (sha256, filename, int(time.time())),
    )
    _get().commit()


# ---------------------------------------------------------------------------
# Events  (direct write; replaces pq_db_ingest.py log-regex approach)
# ---------------------------------------------------------------------------

def add_event(
    kind: str,
    domain: Optional[str] = None,
    url: Optional[str] = None,
    pdf_name: Optional[str] = None,
    details: Optional[str] = None,
) -> None:
    _get().execute(
        "INSERT INTO events(ts,kind,domain,url,pdf_name,details) VALUES(?,?,?,?,?,?)",
        (int(time.time()), kind, domain, url, pdf_name, details),
    )
    _get().commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _domain(url: str) -> Optional[str]:
    try:
        return url.split("://", 1)[1].split("/", 1)[0].lower()
    except Exception:
        return None


def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def sha256_text(s: str) -> str:
    return sha256_bytes((s or "").encode("utf-8", errors="ignore"))
