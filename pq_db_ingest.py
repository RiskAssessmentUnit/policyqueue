import os, re, sqlite3, time, hashlib
from pathlib import Path

ROOT = Path(os.environ.get("PQ_ROOT", str(Path.home() / "policyqueue")))
DB   = Path(os.environ.get("PQ_DB", str(ROOT / "pq.sqlite")))

FOLDERS = {
  "inbox": ROOT / "inbox",
  "bigpdfs": ROOT / "bigpdfs",
  "posts": ROOT / "posts",
  "approved": ROOT / "approved",
  "archive": ROOT / "archive",
  "junk": ROOT / "junk",
}

LOGS = ROOT / "logs"
RUNNER_LOG = LOGS / "runner.log"

SAVED_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+SAVED\s+(.+?)\s+\(\d+\s+bytes\)\s+<-\s+(https?://\S+)", re.M)
DUP_RE   = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+DUP\s+content already saved as\s+(.+?):\s+(https?://\S+)", re.M)
ERR_RE   = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+(HTTPError\s+(\d+)\s+(\S+)|URLError\s+(\S+):\s+(.+)|ERROR fetch\s+(\S+):\s+(.+))", re.M)
PROC_START_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+PROCESS start\s+(.+?\.pdf)\s+bytes=(\d+)", re.M)
PROC_POSTED_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+PROCESS posted\s+(.+?\.pdf)\s+score=(\d+)\s+->\s+(\S+\.post\.txt)", re.M)
PROC_LOW_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+PROCESS low-signal\s+(.+?\.pdf)\s+score=(\d+)\s+->\s+skip", re.M)
CYCLE_DONE_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+CYCLE done\s+(.+)$", re.M)
TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+", re.M)

def ts_to_epoch(s: str) -> int:
    # log timestamps are local-time style; this is fine for trend/stats
    try:
        return int(time.mktime(time.strptime(s, "%Y-%m-%d %H:%M:%S")))
    except Exception:
        return int(time.time())

def domain_of(url: str) -> str:
    try:
        return url.split("://",1)[1].split("/",1)[0].lower()
    except Exception:
        return None

def sha256_of_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def upsert_url(con, url, status="ok", http_code=None, err=None, seen_ts=None):
    d = domain_of(url)
    now = seen_ts or int(time.time())
    con.execute("""
      INSERT INTO urls(url, domain, first_seen_ts, last_seen_ts, status, last_http_code, last_error)
      VALUES(?,?,?,?,?,?,?)
      ON CONFLICT(url) DO UPDATE SET
        domain=excluded.domain,
        last_seen_ts=excluded.last_seen_ts,
        status=excluded.status,
        last_http_code=COALESCE(excluded.last_http_code, urls.last_http_code),
        last_error=COALESCE(excluded.last_error, urls.last_error)
    """, (url, d, now, now, status, http_code, err))
    return d

def upsert_pdf(con, sha, bytes_, filename, source_url=None, saved_path=None, stage="unknown", score=None, processed_ts=None, post_path=None, seen_ts=None):
    now = seen_ts or int(time.time())
    dom = domain_of(source_url) if source_url else None
    con.execute("""
      INSERT INTO pdfs(sha256, bytes, filename, source_url, domain, saved_path, first_seen_ts, last_seen_ts, stage, signal_score, last_processed_ts, last_post_path)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(sha256) DO UPDATE SET
        bytes=COALESCE(excluded.bytes, pdfs.bytes),
        filename=COALESCE(excluded.filename, pdfs.filename),
        source_url=COALESCE(excluded.source_url, pdfs.source_url),
        domain=COALESCE(excluded.domain, pdfs.domain),
        saved_path=COALESCE(excluded.saved_path, pdfs.saved_path),
        last_seen_ts=excluded.last_seen_ts,
        stage=COALESCE(excluded.stage, pdfs.stage),
        signal_score=COALESCE(excluded.signal_score, pdfs.signal_score),
        last_processed_ts=COALESCE(excluded.last_processed_ts, pdfs.last_processed_ts),
        last_post_path=COALESCE(excluded.last_post_path, pdfs.last_post_path)
    """, (sha, bytes_, filename, source_url, dom, saved_path, now, now, stage, score, processed_ts, post_path))

def add_event(con, ts_epoch, kind, domain=None, url=None, pdf_name=None, details=None):
    con.execute("INSERT INTO events(ts,kind,domain,url,pdf_name,details) VALUES(?,?,?,?,?,?)",
                (ts_epoch, kind, domain, url, pdf_name, details))

def infer_stage(p: Path) -> str:
    for k, folder in FOLDERS.items():
        try:
            if p.resolve().parent == folder.resolve():
                return k
        except Exception:
            pass
    return "unknown"

def ingest_files(con):
    count = 0
    for stage, folder in FOLDERS.items():
        if not folder.exists():
            continue
        # PDFs
        for pdf in folder.glob("*.pdf"):
            try:
                st = pdf.stat()
                sha = sha256_of_file(pdf)
                upsert_pdf(con, sha, st.st_size, pdf.name, source_url=None, saved_path=str(pdf), stage=stage, seen_ts=int(st.st_mtime))
                count += 1
            except Exception:
                continue

        # Posts (link by filename stem if possible)
        if stage == "posts":
            for post in folder.glob("*.post.txt"):
                try:
                    # We don't always have sha for the matching pdf; store a lightweight event for now.
                    st = post.stat()
                    add_event(con, int(st.st_mtime), "POST_FILE", None, None, post.name.replace(".post.txt",".pdf"), f"path={post}")
                except Exception:
                    continue
    return count

def ingest_runner_log(con):
    if not RUNNER_LOG.exists():
        return 0
    data = RUNNER_LOG.read_text(encoding="utf-8", errors="ignore")
    # We’ll insert events; cheap and useful.
    inserted = 0

    # SAVED
    for m in SAVED_RE.finditer(data):
        # find ts on same line
        line_start = data.rfind("\n", 0, m.start()) + 1
        line_end = data.find("\n", m.start()); line_end = len(data) if line_end == -1 else line_end
        line = data[line_start:line_end]
        tsm = TS_RE.match(line)
        t = ts_to_epoch(tsm.group(1)) if tsm else int(time.time())

        name = m.group(1).strip()
        url  = m.group(2).strip()
        dom  = upsert_url(con, url, status="ok", seen_ts=t)
        add_event(con, t, "SAVED", dom, url, name, None)
        inserted += 1

    # DUP
    for m in DUP_RE.finditer(data):
        line_start = data.rfind("\n", 0, m.start()) + 1
        line_end = data.find("\n", m.start()); line_end = len(data) if line_end == -1 else line_end
        line = data[line_start:line_end]
        tsm = TS_RE.match(line)
        t = ts_to_epoch(tsm.group(1)) if tsm else int(time.time())

        saved_as = m.group(1).strip()
        url = m.group(2).strip()
        dom = upsert_url(con, url, status="ok", seen_ts=t)
        add_event(con, t, "DUP", dom, url, saved_as, None)
        inserted += 1

    # ERR
    for m in ERR_RE.finditer(data):
        line_start = data.rfind("\n", 0, m.start()) + 1
        line_end = data.find("\n", m.start()); line_end = len(data) if line_end == -1 else line_end
        line = data[line_start:line_end]
        tsm = TS_RE.match(line)
        t = ts_to_epoch(tsm.group(1)) if tsm else int(time.time())

        http_code = None
        url = None
        err = None
        if m.group(2) and m.group(3):
            http_code = int(m.group(2))
            url = m.group(3)
            err = m.group(1)
        elif m.group(4) and m.group(5):
            url = m.group(4)
            err = m.group(5)
        elif m.group(6) and m.group(7):
            url = m.group(6)
            err = m.group(7)

        dom = domain_of(url) if url else None
        if url:
            upsert_url(con, url, status="error", http_code=http_code, err=err, seen_ts=t)
        add_event(con, t, "ERROR", dom, url, None, err)
        inserted += 1

    # PROCESS start/posted/low-signal + cycle done
    for m in PROC_START_RE.finditer(data):
        line_start = data.rfind("\n", 0, m.start()) + 1
        line_end = data.find("\n", m.start()); line_end = len(data) if line_end == -1 else line_end
        line = data[line_start:line_end]
        tsm = TS_RE.match(line)
        t = ts_to_epoch(tsm.group(1)) if tsm else int(time.time())
        pdf = m.group(1).strip()
        add_event(con, t, "PROCESS_START", None, None, pdf, f"bytes={m.group(2)}")
        inserted += 1

    for m in PROC_POSTED_RE.finditer(data):
        line_start = data.rfind("\n", 0, m.start()) + 1
        line_end = data.find("\n", m.start()); line_end = len(data) if line_end == -1 else line_end
        line = data[line_start:line_end]
        tsm = TS_RE.match(line)
        t = ts_to_epoch(tsm.group(1)) if tsm else int(time.time())
        pdf = m.group(1).strip()
        score = int(m.group(2))
        post = m.group(3).strip()
        add_event(con, t, "PROCESS_POSTED", None, None, pdf, f"score={score} post={post}")
        inserted += 1

    for m in PROC_LOW_RE.finditer(data):
        line_start = data.rfind("\n", 0, m.start()) + 1
        line_end = data.find("\n", m.start()); line_end = len(data) if line_end == -1 else line_end
        line = data[line_start:line_end]
        tsm = TS_RE.match(line)
        t = ts_to_epoch(tsm.group(1)) if tsm else int(time.time())
        pdf = m.group(1).strip()
        score = int(m.group(2))
        add_event(con, t, "PROCESS_LOW_SIGNAL", None, None, pdf, f"score={score}")
        inserted += 1

    for m in CYCLE_DONE_RE.finditer(data):
        line_start = data.rfind("\n", 0, m.start()) + 1
        line_end = data.find("\n", m.start()); line_end = len(data) if line_end == -1 else line_end
        line = data[line_start:line_end]
        tsm = TS_RE.match(line)
        t = ts_to_epoch(tsm.group(1)) if tsm else int(time.time())
        add_event(con, t, "CYCLE_DONE", None, None, None, m.group(1).strip())
        inserted += 1

    return inserted

def main():
    if not DB.exists():
        raise SystemExit(f"DB not found: {DB} (run pq_db_init.py first)")

    con = sqlite3.connect(str(DB))
    con.execute("PRAGMA foreign_keys=ON;")
    start = time.time()

    file_rows = ingest_files(con)
    log_rows  = ingest_runner_log(con)

    con.commit()
    con.close()

    dt = time.time() - start
    print(f"OK: ingest complete. files_indexed={file_rows} log_events_added={log_rows} seconds={dt:.2f} db={DB}")

if __name__ == "__main__":
    main()
