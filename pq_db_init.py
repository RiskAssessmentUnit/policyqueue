import os, sqlite3, time
from pathlib import Path

ROOT = Path(os.environ.get("PQ_ROOT", str(Path.home() / "policyqueue")))
DB   = Path(os.environ.get("PQ_DB", str(ROOT / "pq.sqlite")))

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS urls (
  url TEXT PRIMARY KEY,
  domain TEXT,
  first_seen_ts INTEGER,
  last_seen_ts INTEGER,
  status TEXT,              -- ok, error, skip
  last_http_code INTEGER,
  last_error TEXT
);

CREATE TABLE IF NOT EXISTS pdfs (
  pdf_id INTEGER PRIMARY KEY AUTOINCREMENT,
  sha256 TEXT UNIQUE,
  bytes INTEGER,
  filename TEXT,
  source_url TEXT,
  domain TEXT,
  saved_path TEXT,
  first_seen_ts INTEGER,
  last_seen_ts INTEGER,
  stage TEXT,               -- inbox, bigpdfs, posts, approved, archive, junk, unknown
  signal_score INTEGER,     -- last known
  last_processed_ts INTEGER,
  last_post_path TEXT,
  FOREIGN KEY(source_url) REFERENCES urls(url)
);

CREATE INDEX IF NOT EXISTS idx_pdfs_stage ON pdfs(stage);
CREATE INDEX IF NOT EXISTS idx_pdfs_last_seen ON pdfs(last_seen_ts);
CREATE INDEX IF NOT EXISTS idx_pdfs_domain ON pdfs(domain);

CREATE TABLE IF NOT EXISTS events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER,
  kind TEXT,                -- SAVED, DUP, ERROR, PROCESS_START, PROCESS_POSTED, PROCESS_LOW_SIGNAL, CYCLE_DONE
  domain TEXT,
  url TEXT,
  pdf_name TEXT,
  details TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind);
"""

def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB))
    con.executescript(SCHEMA)
    con.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", ("created_ts", str(int(time.time()))))
    con.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", ("schema_version", "1"))
    con.commit()
    con.close()
    print(f"OK: initialized {DB}")

if __name__ == "__main__":
    main()
